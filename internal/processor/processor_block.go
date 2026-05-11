package processor

import (
	"context"
	"log/slog"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/blockstore"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

func (p *Process) fetchReceiptsBatch(ctx context.Context, txs types.Transactions) ([]*types.Receipt, error) {
	receipts := make([]*types.Receipt, len(txs))
	elems := make([]rpc.BatchElem, len(txs))

	for i, tx := range txs {
		elems[i] = rpc.BatchElem{
			Method: "eth_getTransactionReceipt",
			Args:   []interface{}{tx.Hash()},
			Result: &receipts[i],
		}
	}

	if err := p.ethClient.Client().BatchCallContext(ctx, elems); err != nil {
		return nil, err
	}

	return receipts, nil
}

func (p *Process) CalculateBlockTxTip(block *types.Block, txs types.Transactions, receipts []*types.Receipt) blockstore.BlockData {
	blockData := blockstore.BlockData{
		Number:   block.NumberU64(),
		BaseFee:  block.BaseFee(),
		GasLimit: block.GasLimit(),
		Txs:      make([]blockstore.TxInfo, 0, len(txs)),
	}

	for i, tx := range txs {
		if receipts[i] == nil {
			continue
		}

		tip, ok := p.gasanalyzer.EffectiveTip(tx.GasFeeCap(), tx.GasTipCap(), blockData.BaseFee)
		if !ok {
			continue
		}

		weight := p.gasanalyzer.CalculateWeightForGasUsed(receipts[i].GasUsed, blockData.GasLimit)
		blockData.Txs = append(blockData.Txs, blockstore.TxInfo{
			Hash:      tx.Hash().Hex(),
			Tip:       tip,
			GasWeight: weight,
		})
	}

	return blockData
}

func (p *Process) ClearMempool(ctx context.Context, block *types.Block, txs types.Transactions) {
	if len(txs) == 0 {
		return
	}

	//sync.Pool에서 슬라이스 메모리 빌리기
	pSlicePtr := txHashPool.Get().(*[]string)
	txHashes := (*pSlicePtr)[:0]

	for _, tx := range txs {
		txHashes = append(txHashes, tx.Hash().Hex())
	}
	removedCnt := p.state.DeleteBulk(txHashes)

	*pSlicePtr = txHashes     //혹시라도 트랜잭션이 너무 많아 슬라이스의 Capacity가 늘어났다면, 슬라이스 헤더 정보(포인터, 길이, 용량)가 변경
	txHashPool.Put(pSlicePtr) // 반납하기

	if removedCnt > 0 {
		logger.Info(ctx, "Transactions cleared from mempool",
			slog.Uint64("block_number", block.NumberU64()),
			slog.Int("removed_count", removedCnt),
		)
	}
}
