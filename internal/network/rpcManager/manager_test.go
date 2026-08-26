package rpcmanager

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"golang.org/x/time/rate"
)

var errBoom = errors.New("boom")

// --- test helpers ---------------------------------------------------------

// generousLimiter never rate-limits: any non-negative WaitN returns immediately.
func generousLimiter() *rate.Limiter {
	return rate.NewLimiter(rate.Inf, 1000)
}

// exhaustedLimiter rate-limits every positive WaitN call immediately
// (burst 0 < n) without blocking on time.
func exhaustedLimiter() *rate.Limiter {
	return rate.NewLimiter(0, 0)
}

// newFakeClient builds a *Client backed by a nil ethclient (no network) so
// the test fn can identify it by its EthClient pointer. limiter controls the
// Wait() behaviour; limitType defaults to LimitCU.
func newFakeClient(provider string, limiter *rate.Limiter) *Client {
	return &Client{
		EthClient: ethclient.NewClient(nil),
		provider:  provider,
		limitType: LimitCU,
		limiter:   limiter,
	}
}

func newManager(clients []*Client, active int, rotateInterval time.Duration, lastRotateAt time.Time) *RPCManager {
	return &RPCManager{
		clients:        clients,
		active:         active,
		rotateInterval: rotateInterval,
		lastRotateAt:   lastRotateAt,
	}
}

func clientIndex(clients []*Client, target *ethclient.Client) int {
	for i, c := range clients {
		if c.EthClient == target {
			return i
		}
	}
	return -1
}

func clientIndices(clients []*Client, targets []*ethclient.Client) []int {
	indices := make([]int, len(targets))
	for i, t := range targets {
		indices[i] = clientIndex(clients, t)
	}
	return indices
}

// --- EthClientFunc --------------------------------------------------------

func TestEthClientFunc_NoClients(t *testing.T) {
	r := newManager(nil, 0, RotateInterval, time.Now())

	called := false
	err := r.EthClientFunc(context.Background(), RPCCost{CU: 1, RPC: 1}, func(*ethclient.Client) error {
		called = true
		return nil
	})

	if err == nil {
		t.Fatal("EthClientFunc() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "no ethereum rpc client available") {
		t.Fatalf("error = %q, want substring %q", err.Error(), "no ethereum rpc client available")
	}
	if called {
		t.Fatal("fn must not be called when there are no clients")
	}
}

func TestEthClientFunc_Success(t *testing.T) {
	clients := []*Client{
		newFakeClient("alchemy", generousLimiter()),
		newFakeClient("chainstack", generousLimiter()),
		newFakeClient("third", generousLimiter()),
	}

	tests := []struct {
		name           string
		active         int
		rotateInterval time.Duration
		lastRotateAgo  time.Duration
		wantCalledIdx  int
		wantActive     int
	}{
		{
			name:           "no rotation uses active client",
			active:         0,
			rotateInterval: RotateInterval,
			lastRotateAgo:  0,
			wantCalledIdx:  0,
			wantActive:     0,
		},
		{
			name:           "rotates to next after interval",
			active:         0,
			rotateInterval: RotateInterval,
			lastRotateAgo:  2 * RotateInterval,
			wantCalledIdx:  1,
			wantActive:     1,
		},
		{
			name:           "rotation wraps around",
			active:         2,
			rotateInterval: RotateInterval,
			lastRotateAgo:  2 * RotateInterval,
			wantCalledIdx:  0,
			wantActive:     0,
		},
		{
			name:           "zero interval always rotates",
			active:         0,
			rotateInterval: 0,
			lastRotateAgo:  0,
			wantCalledIdx:  1,
			wantActive:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newManager(clients, tt.active, tt.rotateInterval, time.Now().Add(-tt.lastRotateAgo))

			var called *ethclient.Client
			err := r.EthClientFunc(context.Background(), RPCCost{CU: 1, RPC: 1}, func(c *ethclient.Client) error {
				called = c
				return nil
			})

			if err != nil {
				t.Fatalf("EthClientFunc() error = %v, want nil", err)
			}
			if called != clients[tt.wantCalledIdx].EthClient {
				t.Fatalf("called client index = %d, want %d", clientIndex(clients, called), tt.wantCalledIdx)
			}
			if r.active != tt.wantActive {
				t.Fatalf("active = %d, want %d", r.active, tt.wantActive)
			}
		})
	}
}

func TestEthClientFunc_Failover(t *testing.T) {
	clients := []*Client{
		newFakeClient("alchemy", generousLimiter()),
		newFakeClient("chainstack", generousLimiter()),
		newFakeClient("third", generousLimiter()),
	}

	tests := []struct {
		name       string
		active     int
		errs       []error // per client: nil = success
		wantCalled []int
		wantActive int
		wantErr    bool
	}{
		{
			name:       "active fails then next succeeds",
			active:     0,
			errs:       []error{errBoom, nil, nil},
			wantCalled: []int{0, 1},
			wantActive: 1,
		},
		{
			name:       "two failures then success",
			active:     0,
			errs:       []error{errBoom, errBoom, nil},
			wantCalled: []int{0, 1, 2},
			wantActive: 2,
		},
		{
			name:       "failover wraps around",
			active:     2,
			errs:       []error{nil, errBoom, errBoom},
			wantCalled: []int{2, 0},
			wantActive: 0,
		},
		{
			name:       "all providers fail",
			active:     0,
			errs:       []error{errBoom, errBoom, errBoom},
			wantCalled: []int{0, 1, 2},
			wantActive: 0,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newManager(clients, tt.active, RotateInterval, time.Now())

			results := make(map[*ethclient.Client]error, len(clients))
			for i, c := range clients {
				results[c.EthClient] = tt.errs[i]
			}

			var called []*ethclient.Client
			err := r.EthClientFunc(context.Background(), RPCCost{CU: 1, RPC: 1}, func(c *ethclient.Client) error {
				called = append(called, c)
				return results[c]
			})

			if tt.wantErr {
				if err == nil {
					t.Fatal("EthClientFunc() error = nil, want non-nil")
				}
				if !strings.Contains(err.Error(), "all rpc providers failed") {
					t.Fatalf("error = %q, want substring %q", err.Error(), "all rpc providers failed")
				}
			} else if err != nil {
				t.Fatalf("EthClientFunc() error = %v, want nil", err)
			}

			gotCalled := clientIndices(clients, called)
			if len(gotCalled) != len(tt.wantCalled) {
				t.Fatalf("called clients = %v, want %v", gotCalled, tt.wantCalled)
			}
			for i, idx := range tt.wantCalled {
				if gotCalled[i] != idx {
					t.Fatalf("called clients = %v, want %v", gotCalled, tt.wantCalled)
				}
			}

			if r.active != tt.wantActive {
				t.Fatalf("active = %d, want %d", r.active, tt.wantActive)
			}
		})
	}
}

func TestEthClientFunc_FailoverOnRateLimit(t *testing.T) {
	tests := []struct {
		name             string
		activeLimitType  LimitType
		activeLimiter    *rate.Limiter
		healthyLimitType LimitType
		cost             RPCCost
	}{
		{
			name:             "CU limit exceeded",
			activeLimitType:  LimitCU,
			activeLimiter:    exhaustedLimiter(),
			healthyLimitType: LimitCU,
			cost:             RPCCost{CU: 1, RPC: 1},
		},
		{
			name:             "RPS limit exceeded",
			activeLimitType:  LimitRPS,
			activeLimiter:    exhaustedLimiter(),
			healthyLimitType: LimitRPS,
			cost:             RPCCost{CU: 1, RPC: 1},
		},
		{
			name:             "unsupported limit type",
			activeLimitType:  LimitType(99),
			activeLimiter:    generousLimiter(),
			healthyLimitType: LimitCU,
			cost:             RPCCost{CU: 1, RPC: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			active := &Client{
				EthClient: ethclient.NewClient(nil),
				provider:  "alchemy",
				limitType: tt.activeLimitType,
				limiter:   tt.activeLimiter,
			}
			healthy := &Client{
				EthClient: ethclient.NewClient(nil),
				provider:  "chainstack",
				limitType: tt.healthyLimitType,
				limiter:   generousLimiter(),
			}
			r := newManager([]*Client{active, healthy}, 0, RotateInterval, time.Now())

			var called *ethclient.Client
			err := r.EthClientFunc(context.Background(), tt.cost, func(c *ethclient.Client) error {
				called = c
				return nil
			})

			if err != nil {
				t.Fatalf("EthClientFunc() error = %v, want nil", err)
			}
			if called != healthy.EthClient {
				t.Fatal("must failover to the healthy client when active Wait fails")
			}
			if r.active != 1 {
				t.Fatalf("active = %d, want 1", r.active)
			}
		})
	}
}

func TestEthClientFunc_AllClientsRateLimited(t *testing.T) {
	limitedA := &Client{EthClient: ethclient.NewClient(nil), provider: "a", limitType: LimitCU, limiter: exhaustedLimiter()}
	limitedB := &Client{EthClient: ethclient.NewClient(nil), provider: "b", limitType: LimitCU, limiter: exhaustedLimiter()}
	r := newManager([]*Client{limitedA, limitedB}, 0, RotateInterval, time.Now())

	called := false
	err := r.EthClientFunc(context.Background(), RPCCost{CU: 1, RPC: 1}, func(*ethclient.Client) error {
		called = true
		return nil
	})

	if err == nil {
		t.Fatal("EthClientFunc() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "all rpc providers failed") {
		t.Fatalf("error = %q, want containing %q", err.Error(), "all rpc providers failed")
	}
	if called {
		t.Fatal("fn must not be called when all clients are rate limited")
	}
}

func TestEthClientFunc_NilActiveClientFailsover(t *testing.T) {
	nilActive := &Client{
		EthClient: nil,
		provider:  "broken",
		limitType: LimitCU,
		limiter:   generousLimiter(),
	}
	healthy := newFakeClient("chainstack", generousLimiter())
	r := newManager([]*Client{nilActive, healthy}, 0, RotateInterval, time.Now())

	var called *ethclient.Client
	err := r.EthClientFunc(context.Background(), RPCCost{CU: 1, RPC: 1}, func(c *ethclient.Client) error {
		called = c
		return nil
	})

	if err != nil {
		t.Fatalf("EthClientFunc() error = %v, want nil", err)
	}
	if called != healthy.EthClient {
		t.Fatal("must failover to the healthy client when active is nil")
	}
	if r.active != 1 {
		t.Fatalf("active = %d, want 1", r.active)
	}
}

func TestEthClientFunc_AllClientsNil(t *testing.T) {
	nilA := &Client{EthClient: nil, provider: "a", limitType: LimitCU, limiter: generousLimiter()}
	nilB := &Client{EthClient: nil, provider: "b", limitType: LimitCU, limiter: generousLimiter()}
	r := newManager([]*Client{nilA, nilB}, 0, RotateInterval, time.Now())

	called := false
	err := r.EthClientFunc(context.Background(), RPCCost{CU: 1, RPC: 1}, func(c *ethclient.Client) error {
		called = true
		return nil
	})

	if err == nil {
		t.Fatal("EthClientFunc() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "all rpc providers failed") {
		t.Fatalf("error = %q, want containing %q", err.Error(), "all rpc providers failed")
	}
	if called {
		t.Fatal("fn must not be called when all clients are nil")
	}
}

func TestEthClientFunc_FailoverSkipsNilEthClient(t *testing.T) {
	first := newFakeClient("alchemy", generousLimiter())
	broken := &Client{
		EthClient: nil,
		provider:  "broken",
		limitType: LimitCU,
		limiter:   generousLimiter(),
	}
	last := newFakeClient("chainstack", generousLimiter())
	r := newManager([]*Client{first, broken, last}, 0, RotateInterval, time.Now())

	results := map[*ethclient.Client]error{
		first.EthClient: errBoom,
		last.EthClient:  nil,
	}

	var called []*ethclient.Client
	err := r.EthClientFunc(context.Background(), RPCCost{CU: 1, RPC: 1}, func(c *ethclient.Client) error {
		called = append(called, c)
		return results[c]
	})

	if err != nil {
		t.Fatalf("EthClientFunc() error = %v, want nil", err)
	}
	if len(called) != 2 || called[0] != first.EthClient || called[1] != last.EthClient {
		t.Fatalf("called clients = %v, want [first, last] (broken skipped)", clientIndices([]*Client{first, broken, last}, called))
	}
	if r.active != 2 {
		t.Fatalf("active = %d, want 2", r.active)
	}
}

// --- Client.Wait ----------------------------------------------------------

func TestClient_Wait(t *testing.T) {
	tests := []struct {
		name      string
		limitType LimitType
		limiter   *rate.Limiter
		cost      RPCCost
		wantErr   bool
	}{
		{name: "CU success", limitType: LimitCU, limiter: generousLimiter(), cost: RPCCost{CU: 1, RPC: 1}, wantErr: false},
		{name: "CU rate limited", limitType: LimitCU, limiter: exhaustedLimiter(), cost: RPCCost{CU: 1, RPC: 1}, wantErr: true},
		{name: "RPS success", limitType: LimitRPS, limiter: generousLimiter(), cost: RPCCost{CU: 1, RPC: 1}, wantErr: false},
		{name: "RPS rate limited", limitType: LimitRPS, limiter: exhaustedLimiter(), cost: RPCCost{CU: 1, RPC: 1}, wantErr: true},
		{name: "zero cost bypasses limit", limitType: LimitCU, limiter: exhaustedLimiter(), cost: RPCCost{CU: 0, RPC: 0}, wantErr: false},
		{name: "unsupported limit type", limitType: LimitType(99), limiter: generousLimiter(), cost: RPCCost{CU: 1, RPC: 1}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{limitType: tt.limitType, limiter: tt.limiter}

			err := c.Wait(context.Background(), tt.cost)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Wait() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
