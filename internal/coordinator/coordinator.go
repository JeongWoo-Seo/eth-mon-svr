package coordinator

import (
	"context"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/processor"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type Coordinator struct {
	pending *pendingPipeline
	block   *blockPipeline
}

func NewCoordinator(proc *processor.Process, maxBlockCount int) *Coordinator {
	return &Coordinator{
		pending: newPendingPipeline(pendingWorkerCount, proc),
		block:   newBlockPipeline(proc, maxBlockCount),
	}
}

func (c *Coordinator) Start(ctx context.Context) {
	c.pending.start(ctx)
	c.block.start(ctx)
}

func (c *Coordinator) Stop() {
	c.pending.stop()
	c.block.stop()
}

func (c *Coordinator) PushHeader(header *types.Header) {
	c.block.push(header)
}

func (c *Coordinator) PushTxHash(hash common.Hash) {
	c.pending.push(hash)
}
