package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type AuthGrpcClient struct {
	client pb.AuthServiceClient
	conn   *grpc.ClientConn
}

func NewAuthGrpcClient(addr string) (*AuthGrpcClient, error) {
	if addr == "" {
		return nil, errors.New("auth grpc server address is empty")
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("create auth grpc client: %w", err)
	}
	return &AuthGrpcClient{
		conn:   conn,
		client: pb.NewAuthServiceClient(conn),
	}, nil
}

func (c *AuthGrpcClient) Authenticate(ctx context.Context, clientId, clientSecret string) (string, error) {
	if c == nil || c.client == nil {
		return "", errors.New("auth grpc client is nil")
	}

	if clientId == "" {
		return "", errors.New("client id is empty")
	}

	if clientSecret == "" {
		return "", errors.New("client secret is empty")
	}

	res, err := c.client.Authenticate(ctx, &pb.AuthenticateRequest{
		ClientId:     clientId,
		ClientSecret: clientSecret,
	})
	if err != nil {
		return "", fmt.Errorf("authenticate: %w", err)
	}

	if res.GetAccessToken() == "" {
		return "", errors.New("auth server returned empty access token")
	}

	return res.GetAccessToken(), nil
}

func (c *AuthGrpcClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}

	return c.conn.Close()
}
