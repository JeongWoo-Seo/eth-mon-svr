package grpcClient

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"google.golang.org/grpc"
)

var errBoom = errors.New("boom")

// --- mocks -----------------------------------------------------------------
type fakeGasServiceClient struct {
	feeErr    error
	failures  int
	feeCalls  int
	responses []*pb.CommonResponse
}

func (f *fakeGasServiceClient) UploadFeeBuckets(
	ctx context.Context,
	in *pb.FeeStatisticsRequest,
	opts ...grpc.CallOption,
) (*pb.CommonResponse, error) {
	f.feeCalls++

	// RPC 자체의 에러를 발생시키는 테스트
	if f.feeCalls <= f.failures {
		return nil, f.feeErr
	}

	// 호출별 response가 설정되어 있으면 해당 response 반환
	index := f.feeCalls - f.failures - 1
	if index >= 0 && index < len(f.responses) {
		return f.responses[index], nil
	}

	// 기본 성공
	return &pb.CommonResponse{
		Success: true,
		Code:    pb.ResponseCode_SUCCESS,
	}, nil
}

func (f *fakeGasServiceClient) UploadGasPredictions(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[pb.GasPredictionStream, pb.GasPredictionResponse], error) {
	return nil, nil
}

func predictionStream(block uint64) *pb.GasPredictionStream {
	return &pb.GasPredictionStream{
		Event: &pb.GasPredictionStream_Prediction{
			Prediction: &pb.GasPredictionRequest{NextBlockNumber: block},
		},
	}
}

// --- GasPredictResultSend (gap detection) ----------------------------------

func TestGasPredictResultSend(t *testing.T) {
	tests := []struct {
		name      string
		lastBlock uint64
		nextBlock uint64
		wantGap   bool
		wantFrom  uint64
		wantTo    uint64
	}{
		{name: "first block never gaps", lastBlock: 0, nextBlock: 10, wantGap: false},
		{name: "consecutive block never gaps", lastBlock: 9, nextBlock: 10, wantGap: false},
		{name: "jump injects gap", lastBlock: 10, nextBlock: 15, wantGap: true, wantFrom: 11, wantTo: 14},
		{name: "single missing block", lastBlock: 10, nextBlock: 12, wantGap: true, wantFrom: 11, wantTo: 11},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &GasPredictionClient{
				GasPredictCh:            make(chan *pb.GasPredictionStream, 10),
				gasPreidctLastSentBlock: tt.lastBlock,
			}

			c.GasPredictResultSend(predictionStream(tt.nextBlock))

			if tt.wantGap {
				gap := <-c.GasPredictCh
				ge := gap.GetGap()
				if ge == nil {
					t.Fatal("expected a gap event first")
				}
				if ge.FromBlock != tt.wantFrom || ge.ToBlock != tt.wantTo {
					t.Fatalf("gap = [%d,%d], want [%d,%d]", ge.FromBlock, ge.ToBlock, tt.wantFrom, tt.wantTo)
				}
				if ge.Reason != pb.GapEvent_RECONNECT {
					t.Fatalf("gap reason = %v, want RECONNECT", ge.Reason)
				}
			}

			got := <-c.GasPredictCh
			if got.GetPrediction() == nil || got.GetPrediction().NextBlockNumber != tt.nextBlock {
				t.Fatalf("expected prediction %d after gap logic", tt.nextBlock)
			}
		})
	}
}

func TestGasPredictResultSend_NilPrediction(t *testing.T) {
	c := &GasPredictionClient{
		GasPredictCh:            make(chan *pb.GasPredictionStream, 10),
		gasPreidctLastSentBlock: 10,
	}

	// stream message with no prediction event must be ignored
	c.GasPredictResultSend(&pb.GasPredictionStream{})

	select {
	case item := <-c.GasPredictCh:
		t.Fatalf("expected nothing enqueued, got %v", item)
	default:
	}
}

// --- sendFeeBucket (unary send with retry) ----------------------------------
func TestSendFeeBucket(t *testing.T) {
	t.Run("success on first try", func(t *testing.T) {
		fake := &fakeGasServiceClient{}
		c := &GasPredictionClient{
			client: fake,
		}

		err := c.sendFeeBucket(context.Background(), &pb.FeeStatisticsRequest{})
		if err != nil {
			t.Fatalf("sendFeeBucket() error = %v, want nil", err)
		}
		if fake.feeCalls != 1 {
			t.Fatalf("fee calls = %d, want 1", fake.feeCalls)
		}
	})

	t.Run("rpc error retry then success", func(t *testing.T) {
		expectedErr := errors.New("temporary rpc error")

		fake := &fakeGasServiceClient{
			failures: 2,
			feeErr:   expectedErr,
		}

		c := &GasPredictionClient{
			client:          fake,
			unaryRetryDelay: 0,
		}

		err := c.sendFeeBucket(context.Background(), &pb.FeeStatisticsRequest{})
		if err != nil {
			t.Fatalf("sendFeeBucket() error = %v, want nil", err)
		}

		// 1, 2회 실패 -> 3회 성공
		if fake.feeCalls != 3 {
			t.Fatalf("fee calls = %d, want 3", fake.feeCalls)
		}
	})

	t.Run("rpc error all attempts fail", func(t *testing.T) {
		expectedErr := errors.New("rpc unavailable")

		fake := &fakeGasServiceClient{
			failures: maxFeeBucketAttempts,
			feeErr:   expectedErr,
		}

		c := &GasPredictionClient{
			client:          fake,
			unaryRetryDelay: 0,
		}

		err := c.sendFeeBucket(context.Background(), &pb.FeeStatisticsRequest{})
		if err == nil {
			t.Fatal("sendFeeBucket() error = nil, want error")
		}

		if !errors.Is(err, expectedErr) {
			t.Fatalf(
				"sendFeeBucket() error = %v, want %v",
				err,
				expectedErr,
			)
		}

		if fake.feeCalls != maxFeeBucketAttempts {
			t.Fatalf(
				"fee calls = %d, want %d",
				fake.feeCalls,
				maxFeeBucketAttempts,
			)
		}
	})

	t.Run("invalid request does not retry", func(t *testing.T) {
		fake := &fakeGasServiceClient{
			responses: []*pb.CommonResponse{
				{
					Success: false,
					Code:    pb.ResponseCode_INVALID_REQUEST,
					Message: "invalid request",
				},
			},
		}

		c := &GasPredictionClient{
			client: fake,
		}

		err := c.sendFeeBucket(context.Background(), &pb.FeeStatisticsRequest{})
		if err == nil {
			t.Fatal("sendFeeBucket() error = nil, want error")
		}

		if !strings.Contains(err.Error(), "invalid request") {
			t.Fatalf(
				"sendFeeBucket() error = %v, want invalid request",
				err,
			)
		}

		// INVALID_REQUEST는 retry하지 않아야 함
		if fake.feeCalls != 1 {
			t.Fatalf(
				"fee calls = %d, want 1",
				fake.feeCalls,
			)
		}
	})

	t.Run("server rejection all attempts fail", func(t *testing.T) {
		responses := make([]*pb.CommonResponse, maxFeeBucketAttempts)

		for i := 0; i < maxFeeBucketAttempts; i++ {
			responses[i] = &pb.CommonResponse{
				Success: false,
				Code:    pb.ResponseCode_INTERNAL_ERROR,
				Message: "server error",
			}
		}

		fake := &fakeGasServiceClient{
			responses: responses,
		}

		c := &GasPredictionClient{
			client:          fake,
			unaryRetryDelay: 0,
		}

		err := c.sendFeeBucket(context.Background(), &pb.FeeStatisticsRequest{})
		if err == nil {
			t.Fatal("sendFeeBucket() error = nil, want error")
		}

		if !strings.Contains(err.Error(), "server error") {
			t.Fatalf(
				"sendFeeBucket() error = %v, want server error",
				err,
			)
		}

		if fake.feeCalls != maxFeeBucketAttempts {
			t.Fatalf(
				"fee calls = %d, want %d",
				fake.feeCalls,
				maxFeeBucketAttempts,
			)
		}
	})
}

func TestSendFeeBucket_RetryThenSuccess(t *testing.T) {
	fake := &fakeGasServiceClient{feeErr: errBoom, failures: 2}
	c := &GasPredictionClient{client: fake, unaryRetryDelay: time.Millisecond}

	err := c.sendFeeBucket(context.Background(), &pb.FeeStatisticsRequest{})

	if err != nil {
		t.Fatalf("sendFeeBucket() error = %v, want nil", err)
	}
	if fake.feeCalls != 3 {
		t.Fatalf("fee calls = %d, want 3", fake.feeCalls)
	}
}

func TestSendFeeBucket_ExhaustsAttempts(t *testing.T) {
	fake := &fakeGasServiceClient{feeErr: errBoom, failures: 100}
	c := &GasPredictionClient{client: fake, unaryRetryDelay: time.Millisecond}

	err := c.sendFeeBucket(context.Background(), &pb.FeeStatisticsRequest{})

	if !errors.Is(err, errBoom) {
		t.Fatalf("sendFeeBucket() error = %v, want errBoom", err)
	}
	if fake.feeCalls != maxFeeBucketAttempts {
		t.Fatalf("fee calls = %d, want %d", fake.feeCalls, maxFeeBucketAttempts)
	}
}
