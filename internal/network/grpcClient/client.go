package grpcClient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/network/auth"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	gasPredictionBufferSize = 300
	feeBucketBufferSize     = 1
	streamReconnectDelay    = 2 * time.Second
	unaryRetryDelay         = 2 * time.Second
	streamFastRetryCount    = 3
	streamFastRetryDelay    = 2 * time.Second
	streamMaxRetryDelay     = 30 * time.Second
	maxFeeBucketAttempts    = 3
)

type GasPredictionClient struct {
	client       pb.GasPredictionServiceClient
	conn         *grpc.ClientConn
	GasPredictCh chan *pb.GasPredictionStream
	FeeBucketCh  chan *pb.FeeStatisticsRequest

	mu                      sync.Mutex
	gasPreidctLastSentBlock uint64

	unaryRetryDelay time.Duration
}

func NewGasPredictClient(ctx context.Context, addr string, tokenManager *auth.TokenManager) (*GasPredictionClient, func(), error) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(auth.UnaryClientInterceptor(tokenManager)),
		grpc.WithStreamInterceptor(auth.StreamClientInterceptor(tokenManager)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	c := &GasPredictionClient{
		client:                  pb.NewGasPredictionServiceClient(cc),
		conn:                    cc,
		GasPredictCh:            make(chan *pb.GasPredictionStream, gasPredictionBufferSize),
		FeeBucketCh:             make(chan *pb.FeeStatisticsRequest, feeBucketBufferSize),
		gasPreidctLastSentBlock: 0,
		unaryRetryDelay:         unaryRetryDelay,
	}

	grpcCtx, cancel := context.WithCancel(ctx)

	//stream worker
	go c.startStreamWorker(grpcCtx)

	//unary worker
	go c.startUnaryWoker(grpcCtx)

	closeClient := func() {
		//worker 종료
		cancel()

		if err := cc.Close(); err != nil {
			logger.Error(context.Background(), "failed to close grpc connection",
				err,
				slog.String("system", "grpc client"))
		}
	}

	return c, closeClient, nil
}

//stream

func (c *GasPredictionClient) startStreamWorker(ctx context.Context) {
	retryCount := 0
	retryDelay := streamFastRetryDelay

	for {
		// ctx 종료 신호
		if ctx.Err() != nil {
			logger.Info(ctx, "stop stream worker",
				slog.String("system", "grpc client"))

			return
		}

		stream, err := c.client.UploadGasPredictions(ctx)
		if err == nil {
			logger.Info(ctx, "stream connected successfully",
				slog.String("system", "grpc client"))

			retryCount = 0
			retryDelay = streamFastRetryDelay

			streamErr := c.processStream(ctx, stream)
			//ctx 종료 시
			if ctx.Err() != nil {
				return
			}

			logger.Warn(ctx, "stream disconnected, reconnecting",
				slog.String("system", "grpc client"),
				slog.Any("err", streamErr))
		} else {
			if ctx.Err() != nil {
				return
			}

			logger.Error(ctx, "failed to connect stream",
				err,
				slog.String("system", "grpc client"))
		}

		//reconnect and retry delay
		if retryCount < streamFastRetryCount {
			retryCount++

			if !WaitForRetry(ctx, streamFastRetryDelay) {
				return
			}

			continue
		}

		if !WaitForRetry(ctx, retryDelay) {
			return
		}

		retryDelay *= 2
		if retryDelay > streamMaxRetryDelay {
			retryDelay = streamMaxRetryDelay
		}
	}
}

func (c *GasPredictionClient) processStream(ctx context.Context, stream pb.GasPredictionService_UploadGasPredictionsClient) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case req, ok := <-c.GasPredictCh:
			if !ok {
				return nil
			}

			if req == nil {
				continue
			}

			//스트림 메시지 전송
			if err := stream.Send(req); err != nil {
				logger.Error(ctx, "failed to send gas prediction via stream",
					err,
					slog.String("system", "grpc client"))
				return err
			}

			res, err := stream.Recv()
			if err != nil {
				logger.Error(ctx, "failed to recv gas prediction via stream",
					err,
					slog.String("system", "grpc client"))
				return err
			}

			if !res.Success {
				return fmt.Errorf("server rejected: %s", res.Message)
			}

			if p := req.GetPrediction(); p != nil {
				c.mu.Lock()
				c.gasPreidctLastSentBlock = p.NextBlockNumber
				c.mu.Unlock()
			}
		}
	}
}

func (c *GasPredictionClient) GasPredictResultSend(req *pb.GasPredictionStream) {
	p := req.GetPrediction()
	if p == nil {
		return
	}

	c.mu.Lock()
	last := c.gasPreidctLastSentBlock
	c.mu.Unlock()

	if last != 0 && p.NextBlockNumber > last+1 {
		gap := &pb.GasPredictionStream{
			Event: &pb.GasPredictionStream_Gap{
				Gap: &pb.GapEvent{
					FromBlock: last + 1,
					ToBlock:   p.NextBlockNumber - 1,
					Reason:    pb.GapEvent_RECONNECT,
				},
			},
		}
		c.enqueue(gap)
	}
	c.enqueue(req)
}

func (c *GasPredictionClient) enqueue(req *pb.GasPredictionStream) {
	select {
	case c.GasPredictCh <- req:

	// 버퍼가 가득찰 경우
	default:
		logger.Warn(context.Background(), "send channel full and GasPredictionRequest dropp the data",
			slog.String("system", "grpc client"))
	}
}

//Unary

func (c *GasPredictionClient) startUnaryWoker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "stop unary worker",
				slog.String("system", "grpc client"))
			return

		case req, ok := <-c.FeeBucketCh:
			if !ok {
				logger.Info(ctx, "fee bucket channel closed",
					slog.String("system", "grpc client"))
				return
			}

			if err := c.sendFeeBucket(ctx, req); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}

				logger.Error(ctx, "failed to send fee statistics after max attempts",
					err,
					slog.String("system", "grpc client"),
					slog.Int("max_attempts", maxFeeBucketAttempts),
				)
			}
		}
	}
}

func (c *GasPredictionClient) sendFeeBucket(ctx context.Context, req *pb.FeeStatisticsRequest) error {
	var lastErr error

	delay := c.unaryRetryDelay
	if delay <= 0 {
		delay = unaryRetryDelay
	}

	for attempt := 1; attempt <= maxFeeBucketAttempts; attempt++ {
		//종류 ctx가 발생 시
		if ctx.Err() != nil {
			return ctx.Err()
		}

		_, err := c.client.UploadFeeBuckets(ctx, req)
		if err == nil {
			logger.Info(ctx, "fee statistics sent successfully",
				slog.String("system", "grpc client"))

			return nil
		}

		lastErr = err
		// 전송 실패 시 retry
		logger.Error(ctx, "failed to send fee statistics and retry",
			err,
			slog.String("system", "grpc client"),
			slog.Int("attempt", attempt))

		if attempt == maxFeeBucketAttempts {
			break
		}

		if !WaitForRetry(ctx, delay) {
			return ctx.Err()
		}
	}

	return lastErr
}

func (c *GasPredictionClient) FeeBucketSend(req *pb.FeeStatisticsRequest) {
	select {
	case c.FeeBucketCh <- req:
		return
	default:
	}

	// 기존에 대기 중인 오래된 데이터 제거
	select {
	case <-c.FeeBucketCh: // 채널에 데이터가 있으면 하나 제거
	default:
	}

	// 최신 데이터 삽입
	select {
	case c.FeeBucketCh <- req:
	default:
		logger.Warn(context.Background(), "failed to enqueue latest fee statistics",
			slog.String("system", "grpc client"))
	}
}

func WaitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
