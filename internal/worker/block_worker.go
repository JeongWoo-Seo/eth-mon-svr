package worker

import (
	"context"

	"github.com/ethereum/go-ethereum/core/types"
)

const (
	headBufferSize = 100
)

type BlockWorker struct {
	header chan *types.Header
	proc   Processor
}

func NewBlockWorker(proc Processor) *BlockWorker {
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
				//block 데이터 수집 및 상태 업데이트
				b.proc.ProcessBlock(header)
			}
		}
	}()
}

func (b *BlockWorker) Input() chan<- *types.Header {
	return b.header
}
