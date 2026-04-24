package worker

import (
	"context"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Processor interface {
	GetTxInfo(hash common.Hash)
}

type Pool struct {
	workers   int
	jobs      chan string
	proc      Processor
	ethClient *ethclient.Client
	wg        sync.WaitGroup
}

func NewPool(workers int, porc Processor) *Pool {
	return &Pool{
		workers: workers,
		jobs:    make(chan string, 5000),
		proc:    porc,
	}
}

func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
}

func (p *Pool) worker(ctx context.Context) {
	defer p.wg.Done()

	for hash := range p.jobs {
		p.proc.GetTxInfo(common.HexToHash(hash))
	}
}
