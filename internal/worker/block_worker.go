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
				block, success := b.proc.ProcessBlock(header)
				if !success {
					continue
				}

				// 가스fee 예측 분석
				// 멤풀 정리가 끝난 직후, 다음 블록 수집과 병렬로 분석 진행
				go b.proc.AnalyzeGasPrice(block)
			}
		}
	}()
}

func (b *BlockWorker) Input() chan<- *types.Header {
	return b.header
}
