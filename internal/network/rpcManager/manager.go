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
)

type RPCManager struct {
	mu          sync.RWMutex
	primary     *Client
	backup      *Client
	active      *Client
	backupUntil time.Time
}

func NewRpcManager(alcUrl, chaUrl string) (*RPCManager, error) {
	alcClient, err := NewEthClient(ProviderAlchemy, alcUrl)
	if err != nil {
		return nil, err
	}

	chaClinent, err := NewEthClient(ProviderChainstack, chaUrl)
	if err != nil {
		alcClient.Close()
		return nil, err
	}

	return &RPCManager{
		primary: alcClient,
		backup:  chaClinent,
		active:  alcClient,
	}, nil
}

func (r *RPCManager) GetClient() *Client {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.active
}

func (r *RPCManager) GetProvider() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.active == nil {
		return ""
	}

	return r.active.provider
}

func (r *RPCManager) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.primary != nil {
		r.primary.Close()
	}

	if r.backup != nil {
		r.backup.Close()
	}

	r.primary = nil
	r.backup = nil
	r.active = nil
}

func (r *RPCManager) EthClientFunc(
	ctx context.Context,
	fn func(client *ethclient.Client) error,
) error {

	now := time.Now()

	r.mu.Lock()

	if r.active == r.backup &&
		!r.backupUntil.IsZero() &&
		now.After(r.backupUntil) {

		r.active = r.primary
		r.backupUntil = time.Time{}
	}

	client := r.active

	r.mu.Unlock()

	if client == nil || client.EthClient == nil {
		return fmt.Errorf("no ethereum rpc client available")
	}

	// 현재 active client 실행
	if err := fn(client.EthClient); err == nil {
		return nil
	} else if client == r.backup {
		return err
	}

	// Primary → Backup
	r.mu.Lock()

	if r.active == r.primary && r.backup != nil {
		r.active = r.backup
		r.backupUntil = time.Now().Add(1 * time.Minute)
	}

	backup := r.active

	r.mu.Unlock()

	if backup == nil || backup.EthClient == nil {
		return fmt.Errorf("no ethereum rpc client available")
	}

	// Backup 재실행
	return fn(backup.EthClient)
}
