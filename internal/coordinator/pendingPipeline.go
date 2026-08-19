package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/processor"
	"github.com/ethereum/go-ethereum/common"
)

const (
	pendingWorkerCount = 8
	maxBatchSize       = 24
	flushInterval      = 100 * time.Millisecond
	txBufferSize       = 50000
)

type pendingProc interface {
	GetTxInfo(ctx context.Context, hashes []common.Hash)
}

type pendingPipeline struct {
	workers int
	proc    pendingProc
	jobs    chan common.Hash

	wg sync.WaitGroup
}

func newPendingPipeline(workers int, proc *processor.Process) *pendingPipeline {
	return &pendingPipeline{
		workers: workers,
		proc:    proc,
		jobs:    make(chan common.Hash, txBufferSize),
	}
}

func (p *pendingPipeline) start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}

}

func (p *pendingPipeline) stop() {
	close(p.jobs)
	p.wg.Wait()
}

func (p *pendingPipeline) push(hash common.Hash) {
	select {
	case p.jobs <- hash:
	default:
		logger.Warn(context.Background(), "pendingChan is full, and drop tx hash",
			slog.String("system", "pendingpipeline"),
		)
	}

}

func (p *pendingPipeline) worker(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	batch := make([]common.Hash, 0, maxBatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		p.proc.GetTxInfo(ctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			return
		case hash, ok := <-p.jobs:
			if !ok {
				flush()
				return
			}

			batch = append(batch, hash)

			if len(batch) >= maxBatchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}
