package processor

import (
	"context"
	"log/slog"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type Process struct {
	state     *mempool.State
	ethClient *ethclient.Client
}

func NewProcess(state *mempool.State, client *ethclient.Client) *Process {
	return &Process{
		state:     state,
		ethClient: client,
	}
}

func (p *Process) GetTxInfo(hashes []common.Hash) {
	if len(hashes) == 0 {
		return
	}

	elems := make([]rpc.BatchElem, len(hashes))
	results := make([]*types.Transaction, len(hashes))

	for i, hash := range hashes {
		elems[i] = rpc.BatchElem{
			Method: "eth_getTransactionByHash",
			Args:   []interface{}{hash},
			Result: &results[i],
		}
	}

	ctx := context.Background()
	err := p.ethClient.Client().BatchCallContext(ctx, elems)
	if err != nil {
		logger.Error(ctx, "Batch RPC call failed",
			err,
			slog.String("system", "ethereum"),
			slog.String("action", "batch_get_tx"),
			slog.Int("batch_size", len(hashes)),
		)
		return
	}

	for i, elem := range elems {
		if elem.Error != nil {
			logger.Error(ctx, "Failed to fetch transaction in batch",
				elem.Error,
				slog.String("system", "ethereum"),
				slog.String("tx_hash", hashes[i].Hex()),
			)
			continue
		}

		tx := results[i]
		if tx == nil {
			logger.Warn(ctx, "Transaction not found",
				slog.String("system", "ethereum"),
				slog.String("tx_hash", hashes[i].Hex()),
			)
			continue
		}

		report.IncTxFeched(uint64(1))
		p.state.Upset(tx)
	}
}

func (p *Process) GetBlockByHash(header *types.Header) {
	ctx := context.Background()

	block, err := p.ethClient.BlockByHash(ctx, header.Hash())
	if err != nil {
		logger.Error(ctx, "Failed to fetch block by hash",
			err,
			slog.String("system", "ethereum"),
			slog.String("block_hash", header.Hash().Hex()))
		return
	}

	txs := block.Transactions()
	if len(txs) == 0 {
		return
	}

	removedCnt := 0
	for _, tx := range txs {
		if p.state.Delete(tx.Hash().Hex()) {
			removedCnt++
		}
	}

	if removedCnt > 0 {
		logger.Info(ctx, "Transactions cleared from mempool",
			slog.Uint64("block_number", block.NumberU64()),
			slog.Int("removed_count", removedCnt),
		)
	}
}
