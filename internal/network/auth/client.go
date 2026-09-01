package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	caCertFile     = "../certs/ca/cert.pem"
	clientCertFile = "../certs/client/cert.pem"
	clientKeyFile  = "../certs/client/key.pem"
)

type AuthGrpcClient struct {
	client pb.AuthServiceClient
	conn   *grpc.ClientConn
}

func NewAuthGrpcClient(addr string, enableTls bool) (*AuthGrpcClient, error) {
	if addr == "" {
		return nil, errors.New("auth grpc server address is empty")
	}

	var creds credentials.TransportCredentials

	if enableTls {
		// CA 파일 읽어오기
		caCert, err := os.ReadFile(caCertFile)
		if err != nil {
			return nil, fmt.Errorf("read CA certificate: %w", err)
		}

		//CA 인증서를 TLS가 신뢰할 수 있는 CA 목록에 등록
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCert) {
			return nil, errors.New("failed to append CA certificate")
		}

		// Client certificate + private key 인증서 생성
		clientCert, err := tls.LoadX509KeyPair(
			clientCertFile,
			clientKeyFile,
		)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}

		tlsConfig := &tls.Config{
			RootCAs: certPool,
			Certificates: []tls.Certificate{
				clientCert,
			},
			MinVersion: tls.VersionTLS13,
		}

		creds = credentials.NewTLS(tlsConfig)
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
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
