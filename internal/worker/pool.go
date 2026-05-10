package worker

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

const (
	maxBatchSize  = 50
	flushInterval = 200 * time.Millisecond
	txBufferSize  = 50000
)

type Processor interface {
	GetTxInfo(hashes []common.Hash)
	ProcessBlock(header *types.Header) (*types.Block, bool)
	AnalyzeGasPrice(latestBlock *types.Block)
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
		jobs:    make(chan string, txBufferSize),
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

	batch := make([]common.Hash, 0, maxBatchSize)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		p.proc.GetTxInfo(batch)
		batch = batch[:0] // batch 버퍼 0으로 초기화
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case hash, ok := <-p.jobs:
			if !ok { //<-p.jobs 이 닫혔을 때 동작
				flush()
				return
			}
			batch = append(batch, common.HexToHash(hash))

			if len(batch) >= maxBatchSize {
				flush()
				ticker.Reset(flushInterval)
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (p *Pool) Input() chan<- string {
	return p.jobs
}
