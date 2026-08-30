package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/sync/singleflight"
)

const (
	tokenRefreshMargin = 30 * time.Second
)

var (
	ErrAccessTokenEmpty = errors.New("access token is empty")
	ErrAccessTokenExp   = errors.New("access token expiration is missing")
)

type TokenManager struct {
	mu sync.RWMutex

	accessToken string
	expiresAt   time.Time

	clientId     string
	clientSecret string

	authClient *AuthGrpcClient

	refresh singleflight.Group
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
	m.mu.RLock()

	if m.isAccessTokenValid() {
		token := m.accessToken
		m.mu.RUnlock()
		return token, nil
	}
	m.mu.RUnlock()

	result, err, _ := m.refresh.Do("access_token", func() (any, error) {
		// 다른 goroutine이 이미 갱신했는지 다시 확인
		m.mu.RLock()
		if m.isAccessTokenValid() {
			token := m.accessToken
			m.mu.RUnlock()
			return token, nil
		}
		m.mu.RUnlock()

		// 네트워크 호출은 lock 없이
		accessToken, expiresAt, err := m.authenticate(ctx)
		if err != nil {
			return "", err
		}

		// 저장할 때만 lock
		m.mu.Lock()
		m.accessToken = accessToken
		m.expiresAt = expiresAt
		m.mu.Unlock()

		return accessToken, nil
	})

	if err != nil {
		return "", err
	}

	logger.Info(ctx, "new access token acquired successfully",
		slog.String("system", "auth"),
		slog.String("action", "authenticate"),
	)

	return result.(string), nil
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

func (m *TokenManager) authenticate(ctx context.Context) (string, time.Time, error) {
	accessToken, err := m.authClient.Authenticate(ctx, m.clientId, m.clientSecret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to authenticate: %w", err)
	}

	if accessToken == "" {
		return "", time.Time{}, ErrAccessTokenEmpty
	}

	expiresAt, err := parseJWTExpiration(accessToken)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to parse access token expiration: %w", err)
	}

	return accessToken, expiresAt, nil
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
