package auth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// --- mocks -----------------------------------------------------------------

type fakeAuthServiceClient struct {
	token string
	err   error

	calls       int
	gotClientId string
	gotSecret   string
}

func (f *fakeAuthServiceClient) Authenticate(ctx context.Context, in *pb.AuthenticateRequest, opts ...grpc.CallOption) (*pb.AuthenticateResponse, error) {
	f.calls++
	if in != nil {
		f.gotClientId = in.GetClientId()
		f.gotSecret = in.GetClientSecret()
	}
	if f.err != nil {
		return nil, f.err
	}
	return &pb.AuthenticateResponse{AccessToken: f.token}, nil
}

func newTestTokenManager(accessToken string, expiresAt time.Time, fake *fakeAuthServiceClient) *TokenManager {
	return &TokenManager{
		accessToken:  accessToken,
		expiresAt:    expiresAt,
		clientId:     "test-id",
		clientSecret: "test-secret",
		authClient:   &AuthGrpcClient{client: fake},
	}
}

// signedJWT builds a validly-signed (but unverified on parse) JWT carrying the
// given claims, used to exercise parseJWTExpiration.
func signedJWT(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := token.SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("sign jwt: %v", err)
	}
	return s
}

// --- parseJWTExpiration ----------------------------------------------------

func TestParseJWTExpiration(t *testing.T) {
	validExp := float64(1700000000)

	tests := []struct {
		name    string
		token   string
		want    time.Time
		wantErr string
	}{
		{
			name:  "valid expiration",
			token: signedJWT(t, jwt.MapClaims{"exp": validExp}),
			want:  time.Unix(int64(validExp), 0),
		},
		{
			name:    "missing exp claim",
			token:   signedJWT(t, jwt.MapClaims{"sub": "test"}),
			wantErr: "access token expiration is missing",
		},
		{
			name:    "exp is not a number",
			token:   signedJWT(t, jwt.MapClaims{"exp": "not-a-number"}),
			wantErr: "invalid jwt exp type",
		},
		{
			name:    "exp is zero",
			token:   signedJWT(t, jwt.MapClaims{"exp": 0}),
			wantErr: "invalid jwt exp",
		},
		{
			name:    "exp is negative",
			token:   signedJWT(t, jwt.MapClaims{"exp": -1}),
			wantErr: "invalid jwt exp",
		},
		{
			name:    "malformed token",
			token:   "not.a.jwt",
			wantErr: "invalid jwt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseJWTExpiration(tt.token)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("error = nil, want containing %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %q, want containing %q", err.Error(), tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("expiration = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- isAccessTokenValid ----------------------------------------------------

func TestTokenManager_IsAccessTokenValid(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name        string
		accessToken string
		expiresAt   time.Time
		want        bool
	}{
		{name: "empty token", accessToken: "", expiresAt: now.Add(time.Hour), want: false},
		{name: "zero expiration", accessToken: "tok", expiresAt: time.Time{}, want: false},
		{name: "expired", accessToken: "tok", expiresAt: now.Add(-time.Hour), want: false},
		{name: "within refresh margin", accessToken: "tok", expiresAt: now.Add(tokenRefreshMargin - time.Second), want: false},
		{name: "valid far future", accessToken: "tok", expiresAt: now.Add(2 * time.Hour), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &TokenManager{accessToken: tt.accessToken, expiresAt: tt.expiresAt}
			if got := m.isAccessTokenValid(); got != tt.want {
				t.Fatalf("isAccessTokenValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- GetAccessToken --------------------------------------------------------

func TestTokenManager_GetAccessToken(t *testing.T) {
	now := time.Now()
	validToken := signedJWT(t, jwt.MapClaims{"exp": float64(1700000000)})

	tests := []struct {
		name        string
		accessToken string
		expiresAt   time.Time
		fake        *fakeAuthServiceClient
		wantToken   string
		wantCalls   int
		wantErr     string
	}{
		{
			name:        "valid cached token is reused",
			accessToken: "cached-tok",
			expiresAt:   now.Add(time.Hour),
			fake:        &fakeAuthServiceClient{token: "fresh-tok"},
			wantToken:   "cached-tok",
			wantCalls:   0,
		},
		{
			name:        "empty token triggers auth",
			accessToken: "",
			expiresAt:   time.Time{},
			fake:        &fakeAuthServiceClient{token: validToken},
			wantToken:   validToken,
			wantCalls:   1,
		},
		{
			name:        "expired token triggers auth",
			accessToken: "stale-tok",
			expiresAt:   now.Add(-time.Hour),
			fake:        &fakeAuthServiceClient{token: validToken},
			wantToken:   validToken,
			wantCalls:   1,
		},
		{
			name:        "auth failure",
			accessToken: "",
			expiresAt:   time.Time{},
			fake:        &fakeAuthServiceClient{err: errors.New("boom")},
			wantErr:     "failed to authenticate",
		},
		{
			name:        "auth returns empty token",
			accessToken: "",
			expiresAt:   time.Time{},
			fake:        &fakeAuthServiceClient{token: ""},
			wantErr:     "auth server returned empty access token",
		},
		{
			name:        "auth returns invalid jwt",
			accessToken: "",
			expiresAt:   time.Time{},
			fake:        &fakeAuthServiceClient{token: "not.a.jwt"},
			wantErr:     "failed to parse access token expiration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestTokenManager(tt.accessToken, tt.expiresAt, tt.fake)

			got, err := m.GetAccessToken(context.Background())

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("error = nil, want containing %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %q, want containing %q", err.Error(), tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
			if got != tt.wantToken {
				t.Fatalf("token = %q, want %q", got, tt.wantToken)
			}
			if tt.fake.calls != tt.wantCalls {
				t.Fatalf("auth calls = %d, want %d", tt.fake.calls, tt.wantCalls)
			}
		})
	}
}

// --- Authenticate ----------------------------------------------------------

func TestAuthGrpcClient_Authenticate(t *testing.T) {
	tests := []struct {
		name         string
		client       *AuthGrpcClient
		clientId     string
		clientSecret string
		wantToken    string
		wantErr      string
	}{
		{
			name:         "nil client",
			client:       nil,
			clientId:     "id",
			clientSecret: "sec",
			wantErr:      "auth grpc client is nil",
		},
		{
			name:         "nil inner client",
			client:       &AuthGrpcClient{client: nil},
			clientId:     "id",
			clientSecret: "sec",
			wantErr:      "auth grpc client is nil",
		},
		{
			name:         "empty client id",
			client:       &AuthGrpcClient{client: &fakeAuthServiceClient{token: "tok"}},
			clientId:     "",
			clientSecret: "sec",
			wantErr:      "client id is empty",
		},
		{
			name:         "empty client secret",
			client:       &AuthGrpcClient{client: &fakeAuthServiceClient{token: "tok"}},
			clientId:     "id",
			clientSecret: "",
			wantErr:      "client secret is empty",
		},
		{
			name:         "grpc error",
			client:       &AuthGrpcClient{client: &fakeAuthServiceClient{err: errors.New("grpc down")}},
			clientId:     "id",
			clientSecret: "sec",
			wantErr:      "authenticate: grpc down",
		},
		{
			name:         "empty token from server",
			client:       &AuthGrpcClient{client: &fakeAuthServiceClient{token: ""}},
			clientId:     "id",
			clientSecret: "sec",
			wantErr:      "auth server returned empty access token",
		},
		{
			name:         "success",
			client:       &AuthGrpcClient{client: &fakeAuthServiceClient{token: "tok123"}},
			clientId:     "id",
			clientSecret: "sec",
			wantToken:    "tok123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.client.Authenticate(context.Background(), tt.clientId, tt.clientSecret)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("error = nil, want containing %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %q, want containing %q", err.Error(), tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
			if got != tt.wantToken {
				t.Fatalf("token = %q, want %q", got, tt.wantToken)
			}
		})
	}
}

// --- WithBearerToken -------------------------------------------------------

func TestWithBearerToken(t *testing.T) {
	tests := []struct {
		name     string
		before   func() context.Context
		token    string
		wantAuth string
		wantKeep map[string]string
	}{
		{
			name:     "adds bearer to empty context",
			before:   func() context.Context { return context.Background() },
			token:    "tok",
			wantAuth: "Bearer tok",
		},
		{
			name: "preserves existing metadata",
			before: func() context.Context {
				md := metadata.Pairs("x-custom", "v1")
				return metadata.NewOutgoingContext(context.Background(), md)
			},
			token:    "tok",
			wantAuth: "Bearer tok",
			wantKeep: map[string]string{"x-custom": "v1"},
		},
		{
			name: "overwrites existing authorization",
			before: func() context.Context {
				md := metadata.Pairs("authorization", "Bearer old")
				return metadata.NewOutgoingContext(context.Background(), md)
			},
			token:    "new-tok",
			wantAuth: "Bearer new-tok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := WithBearerToken(tt.before(), tt.token)

			md, ok := metadata.FromOutgoingContext(ctx)
			if !ok {
				t.Fatal("expected outgoing metadata in context")
			}

			if got := md.Get("authorization"); len(got) != 1 || got[0] != tt.wantAuth {
				t.Fatalf("authorization = %v, want [%q]", got, tt.wantAuth)
			}

			for k, v := range tt.wantKeep {
				if got := md.Get(k); len(got) != 1 || got[0] != v {
					t.Fatalf("metadata[%q] = %v, want [%q]", k, got, v)
				}
			}
		})
	}
}

// --- interceptors ----------------------------------------------------------

func TestUnaryClientInterceptor(t *testing.T) {
	tests := []struct {
		name         string
		tokenManager *TokenManager
		wantErr      string
	}{
		{
			name:         "injects token and invokes",
			tokenManager: newTestTokenManager("tok123", time.Now().Add(time.Hour), &fakeAuthServiceClient{}),
		},
		{
			name:         "token failure",
			tokenManager: newTestTokenManager("", time.Time{}, &fakeAuthServiceClient{err: errors.New("boom")}),
			wantErr:      "failed to get access token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interceptor := UnaryClientInterceptor(tt.tokenManager)

			invoked := false
			var gotMethod string
			var gotCtx context.Context

			err := interceptor(
				context.Background(),
				"/svc/Method",
				"req", "reply", nil,
				func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					invoked = true
					gotMethod = method
					gotCtx = ctx
					return nil
				},
			)

			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
				}
				if invoked {
					t.Fatal("invoker must not be called on token failure")
				}
				return
			}

			if err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
			if !invoked {
				t.Fatal("invoker was not called")
			}
			if gotMethod != "/svc/Method" {
				t.Fatalf("method = %q, want %q", gotMethod, "/svc/Method")
			}
			md, ok := metadata.FromOutgoingContext(gotCtx)
			if !ok || len(md.Get("authorization")) != 1 || md.Get("authorization")[0] != "Bearer tok123" {
				t.Fatalf("missing bearer token in ctx, md = %v", md)
			}
		})
	}
}

func TestStreamClientInterceptor(t *testing.T) {
	tests := []struct {
		name         string
		tokenManager *TokenManager
		wantErr      string
	}{
		{
			name:         "injects token and streams",
			tokenManager: newTestTokenManager("tok123", time.Now().Add(time.Hour), &fakeAuthServiceClient{}),
		},
		{
			name:         "token failure",
			tokenManager: newTestTokenManager("", time.Time{}, &fakeAuthServiceClient{err: errors.New("boom")}),
			wantErr:      "failed to get access token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interceptor := StreamClientInterceptor(tt.tokenManager)

			streamed := false
			var gotCtx context.Context

			_, err := interceptor(
				context.Background(),
				&grpc.StreamDesc{StreamName: "S"},
				nil,
				"/svc/Stream",
				func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
					streamed = true
					gotCtx = ctx
					return nil, nil
				},
			)

			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
				}
				if streamed {
					t.Fatal("streamer must not be called on token failure")
				}
				return
			}

			if err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
			if !streamed {
				t.Fatal("streamer was not called")
			}
			md, ok := metadata.FromOutgoingContext(gotCtx)
			if !ok || len(md.Get("authorization")) != 1 || md.Get("authorization")[0] != "Bearer tok123" {
				t.Fatalf("missing bearer token in ctx, md = %v", md)
			}
		})
	}
}
