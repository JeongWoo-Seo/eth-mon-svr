package auth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	tokenRefreshMargin = 30 * time.Second
)

var (
	ErrAccessTokenEmpty = errors.New("access token is empty")
	ErrAccessTokenExp   = errors.New("access token expiration is missing")
)

type TokenManager struct {
	mu sync.Mutex

	accessToken string
	expiresAt   time.Time

	clientId     string
	clientSecret string

	authClient *AuthGrpcClient
}

func NewTokenManager(client *AuthGrpcClient, clientId, clientSecret string) (*TokenManager, error) {
	if client == nil {
		return nil, errors.New("auth grpc client is nil")
	}

	if clientId == "" {
		return nil, errors.New("client id is empty")
	}

	if clientSecret == "" {
		return nil, errors.New("client secret is empty")
	}
	return &TokenManager{
		authClient:   client,
		clientId:     clientId,
		clientSecret: clientSecret,
	}, nil
}

func (m *TokenManager) GetAccessToken(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isAccessTokenValid() {
		return m.accessToken, nil
	}

	return m.authenticateLocked(ctx)
}

func (m *TokenManager) isAccessTokenValid() bool {
	if m.accessToken == "" {
		return false
	}

	if m.expiresAt.IsZero() {
		return false
	}

	return time.Now().Add(tokenRefreshMargin).Before(m.expiresAt)
}

func (m *TokenManager) authenticateLocked(ctx context.Context) (string, error) {
	accessToken, err := m.authClient.Authenticate(ctx, m.clientId, m.clientSecret)
	if err != nil {
		return "", fmt.Errorf("failed to authenticate: %w", err)
	}

	if accessToken == "" {
		return "", ErrAccessTokenEmpty
	}

	expiresAt, err := parseJWTExpiration(accessToken)
	if err != nil {
		return "", fmt.Errorf("failed to parse access token expiration: %w", err)
	}

	m.accessToken = accessToken
	m.expiresAt = expiresAt

	return m.accessToken, nil
}

func parseJWTExpiration(accessToken string) (time.Time, error) {
	parser := jwt.NewParser()

	token, _, err := parser.ParseUnverified(accessToken, jwt.MapClaims{})
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid jwt: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return time.Time{}, errors.New("invalid jwt claims")
	}

	exp, ok := claims["exp"]
	if !ok {
		return time.Time{}, ErrAccessTokenExp
	}

	//unix -> time.Time으로 변환
	expFloat, ok := exp.(float64)
	if !ok {
		return time.Time{}, fmt.Errorf("invalid jwt exp type: %T", exp)
	}

	if expFloat <= 0 {
		return time.Time{}, errors.New("invalid jwt exp")
	}

	return time.Unix(int64(expFloat), 0), nil
}
