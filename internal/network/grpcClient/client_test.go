package grpcClient

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"google.golang.org/grpc"
)

var errBoom = errors.New("boom")

// --- mocks -----------------------------------------------------------------

// fakeGasServiceClient implements pb.GasPredictionServiceClient so the unary
// send path can be tested without a real gRPC connection.
type fakeGasServiceClient struct {
	feeErr   error
	failures int // fail the first N calls, then succeed
	feeCalls int
}

func (f *fakeGasServiceClient) UploadGasPredictions(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[pb.GasPredictionStream, pb.GasPredictionResponse], error) {
	return nil, nil
}

func (f *fakeGasServiceClient) UploadFeeBuckets(ctx context.Context, in *pb.FeeStatisticsRequest, opts ...grpc.CallOption) (*pb.CommonResponse, error) {
	f.feeCalls++
	if f.feeCalls <= f.failures {
		return nil, f.feeErr
	}
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
		c := &GasPredictionClient{client: fake}

		err := c.sendFeeBucket(context.Background(), &pb.FeeStatisticsRequest{})

		if err != nil {
			t.Fatalf("sendFeeBucket() error = %v, want nil", err)
		}
		if fake.feeCalls != 1 {
			t.Fatalf("fee calls = %d, want 1", fake.feeCalls)
		}
	})

	t.Run("cancelled context returns context error", func(t *testing.T) {
		fake := &fakeGasServiceClient{}
		c := &GasPredictionClient{client: fake}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := c.sendFeeBucket(ctx, &pb.FeeStatisticsRequest{})

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("sendFeeBucket() error = %v, want context.Canceled", err)
		}
		if fake.feeCalls != 0 {
			t.Fatalf("fee calls = %d, want 0", fake.feeCalls)
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
