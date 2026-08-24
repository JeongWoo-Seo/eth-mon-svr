package auth

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func UnaryClientInterceptor(tokenManager *TokenManager) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		token, err := tokenManager.GetAccessToken(ctx)
		if err != nil {
			return fmt.Errorf("failed to get access token: %w", err)
		}

		ctx = WithBearerToken(ctx, token)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func StreamClientInterceptor(tokenManager *TokenManager) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		token, err := tokenManager.GetAccessToken(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get access token: %w", err)
		}

		ctx = WithBearerToken(ctx, token)

		return streamer(ctx, desc, cc, method, opts...)
	}
}

func WithBearerToken(ctx context.Context, token string) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	} else {
		md = md.Copy()
	}

	md.Set("authorization", "Bearer "+token)

	return metadata.NewOutgoingContext(ctx, md)
}
