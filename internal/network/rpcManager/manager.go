package rpcmanager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

const (
	ProviderAlchemy    string = "alchemy"
	ProviderChainstack string = "chainstack"

	RotateInterval = 30 * time.Second
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
		client, err := NewEthClient(provider, url)
		if err != nil {
			client.Close()
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

func (r *RPCManager) EthClientFunc(ctx context.Context, fn func(client *ethclient.Client) error) error {
	now := time.Now()

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

	// 현재 active client 실행
	if err := fn(client.EthClient); err == nil {
		return nil
	}

	// 실패한 경우 다른 provider 순회
	for i := 1; i < len(r.clients); i++ {
		next := (idx + i) % len(r.clients)
		nextClient := r.clients[next]

		if err := fn(nextClient.EthClient); err == nil {
			r.mu.Lock()
			r.active = next
			r.lastRotateAt = time.Now()
			r.mu.Unlock()
			return nil
		}
	}

	return fmt.Errorf("all rpc providers failed")
}
