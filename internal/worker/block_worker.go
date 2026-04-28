package worker

import (
	"context"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/processor"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	headBufferSize = 100
)

type BlockWorker struct {
	header chan *types.Header
	proc   *processor.Process
}

func NewBlockWorker(proc *processor.Process) *BlockWorker {
	return &BlockWorker{
		header: make(chan *types.Header, headBufferSize),
		proc:   proc,
	}
}

func (b *BlockWorker) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case header, ok := <-b.header:
				if !ok { // b.headers가 닫혔을 때
					return
				}
				b.proc.GetBlockByHash(header)
			}
		}
	}()
}

func (b *BlockWorker) Input() chan<- *types.Header {
	return b.header
}
