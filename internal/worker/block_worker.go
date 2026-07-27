package worker

import (
	"context"
	"sync"

	"github.com/ethereum/go-ethereum/core/types"
)

const (
	headBufferSize = 100
)

type BlockWorker struct {
	header chan *types.Header
	proc   Processor
	wg     sync.WaitGroup
}

func NewBlockWorker(proc Processor) *BlockWorker {
	return &BlockWorker{
		header: make(chan *types.Header, headBufferSize),
		proc:   proc,
	}
}

func (b *BlockWorker) Start(ctx context.Context) {
	b.wg.Add(1)

	go func() {
		defer b.wg.Done()

		for {
			select {
			case <-ctx.Done():
				return

			case header := <-b.header:
				b.proc.ProcessBlock(header)
			}
		}
	}()
}

func (b *BlockWorker) Wait() {
	b.wg.Wait()
}

func (b *BlockWorker) Input() chan<- *types.Header {
	return b.header
}
