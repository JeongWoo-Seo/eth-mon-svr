package grpcClient

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GasPredictionClient struct {
	client pb.GasPredictionServiceClient
	conn   *grpc.ClientConn
	ch     chan *pb.GasPredictionRequest
}

func NewGasPredictClient(ctx context.Context, addr string) (*GasPredictionClient, func(), error) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	c := &GasPredictionClient{
		client: pb.NewGasPredictionServiceClient(cc),
		conn:   cc,
		ch:     make(chan *pb.GasPredictionRequest, 300),
	}

	//stream worker
	go c.startStreamWorker(ctx)

	close := func() {
		cc.Close()
	}

	return c, close, nil
}

func (c *GasPredictionClient) startStreamWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "stop stream worker",
				slog.String("system", "grpc client"))
			return
		default:
		}

		stream, err := c.client.UploadGasPredictions(ctx)
		if err != nil {
			logger.Error(ctx, "failed to connect stream",
				err,
				slog.String("system", "grpc client"))

			select {
			//재연결 시도전 2초 대기
			case <-time.After(2 * time.Second):
				continue
			case <-ctx.Done():
				return
			}
		}

		logger.Info(ctx, "stream connected successfully",
			slog.String("system", "grpc client"))

		streamErr := c.processStream(ctx, stream)
		if streamErr != nil {
			logger.Warn(ctx, "stream disconnected",
				slog.String("system", "grpc client"),
				slog.Any("err", streamErr))
		} else {
			res, closeErr := stream.CloseAndRecv()
			if closeErr != nil && closeErr != io.EOF {
				logger.Error(ctx, "failed to close stream gracefully",
					closeErr,
					slog.String("system", "grpc client"))
			} else if res != nil {
				logger.Info(ctx, "stream closed by server response",
					slog.String("system", "grpc client"),
					slog.String("message", res.GetMessage()))
			}
			return // 워커 고루틴 완전히 종료
		}

		// 에러로 인한 재연결 전 짧은 대기
		time.Sleep(1 * time.Second)
	}
}

func (c *GasPredictionClient) processStream(ctx context.Context, stream pb.GasPredictionService_UploadGasPredictionsClient) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case req, ok := <-c.ch:
			if !ok {
				// c.ch 채널이 close
				return nil
			}

			//스트림 메시지 전송
			if err := stream.Send(req); err != nil {
				logger.Error(ctx, "failed to send gas prediction via stream",
					err,
					slog.String("system", "grpc client"))
				return err
			}
		}
	}
}

func (c *GasPredictionClient) ResultSend(req *pb.GasPredictionRequest) {
	select {
	case c.ch <- req:
	// 버퍼가 가득찰 경우
	default:
		logger.Warn(context.Background(), "send channel full and dropp the data",
			slog.String("system", "grpc client"))
	}
}
