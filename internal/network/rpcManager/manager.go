package rpcmanager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type RPCManager struct {
	mu      sync.RWMutex
	clients []*Client
	active  int

	rotateInterval time.Duration
	lastRotateAt   time.Time
}

func NewRpcManager(rpcs map[string]string) (*RPCManager, error) {
	clients := make([]*Client, 0, len(rpcs))

	for provider, url := range rpcs {
		policy, ok := rpcPolicies[provider]
		if !ok {
			return nil, fmt.Errorf("unsupported rpc provider: %s", provider)
		}

		client, err := NewEthClient(provider, url, policy)
		if err != nil {
			continue
		}
		clients = append(clients, client)
	}

	if len(clients) == 0 {
		return nil, fmt.Errorf("no rpc clients")
	}

	return &RPCManager{
		clients:        clients,
		active:         0,
		rotateInterval: RotateInterval,
		lastRotateAt:   time.Now(),
	}, nil
}

func (r *RPCManager) GetProvider() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.clients) == 0 {
		return ""
	}

	return r.clients[r.active].provider
}

func (r *RPCManager) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, client := range r.clients {
		client.Close()
	}
}

func (r *RPCManager) EthClientFunc(ctx context.Context, cost RPCCost, fn func(client *ethclient.Client) error) error {
	now := time.Now()
	var lastErr error

	r.mu.Lock()
	if len(r.clients) == 0 {
		r.mu.Unlock()
		return fmt.Errorf("no ethereum rpc client available")
	}

	//rotate check
	if now.Sub(r.lastRotateAt) >= r.rotateInterval {
		r.active = (r.active + 1) % len(r.clients)
		r.lastRotateAt = now
	}

	idx := r.active
	client := r.clients[idx]
	r.mu.Unlock()

	// active client가 사용 가능하면 실행, nil·rate limit·실패 시 아래 failover로 넘어감
	if client.EthClient != nil {
		// rpc 통신 limit (초과 시 failover)
		if err := client.Wait(ctx, cost); err == nil {
			if err := fn(client.EthClient); err == nil {
				return nil
			} else {
				lastErr = err
			}
		} else {
			lastErr = err
		}
	}

	// 실패한 경우 다른 provider 순회
	for i := 1; i < len(r.clients); i++ {
		next := (idx + i) % len(r.clients)
		nextClient := r.clients[next]

		if nextClient.EthClient == nil {
			continue
		}

		if err := nextClient.Wait(ctx, cost); err != nil {
			lastErr = err
			continue
		}

		if err := fn(nextClient.EthClient); err == nil {
			r.mu.Lock()
			r.active = next
			r.lastRotateAt = time.Now()
			r.mu.Unlock()
			return nil
		} else {
			lastErr = err
		}
	}

	return fmt.Errorf("all rpc providers failed: %w", lastErr)
}

func (r *RPCManager) FetchBatch(ctx context.Context, cost RPCCost, elems []rpc.BatchElem) error {
	return r.EthClientFunc(ctx, cost, func(client *ethclient.Client) error {
		return client.Client().BatchCallContext(ctx, elems)
	})
}
