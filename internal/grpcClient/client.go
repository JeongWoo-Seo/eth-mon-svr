package grpcClient

import (
	"fmt"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewGasPredictClient(addr string) (pb.GasPredictionServiceClient, func(), error) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	client := pb.NewGasPredictionServiceClient(cc)
	close := func() {
		cc.Close()
	}

	return client, close, nil
}
