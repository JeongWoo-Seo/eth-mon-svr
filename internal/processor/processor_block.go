package processor

import (
	"context"
	"log/slog"
	"sync"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/blockstore"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/gasanalyzer"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

const (
	receiptChunkSize  = 30
	receiptCuPerTx    = 15
	getBlockReceiptCu = 250
)

var minedHashPool = sync.Pool{
	New: func() interface{} {
		// 초기 캡시티는 프로젝트의 평균 블록당 트랙잭션 수
		return make(map[string]struct{}, 256)
	},
}

func (p *Process) fetchReceiptsBatch(ctx context.Context, txs types.Transactions) ([]*types.Receipt, error) {
	if len(txs) == 0 {
		return nil, nil
	}

	receipts := make([]*types.Receipt, 0, txs.Len())

	for i := 0; i < txs.Len(); i += receiptChunkSize {
		end := i + receiptChunkSize
		if end > txs.Len() {
			end = txs.Len()
		}

		chunkTxs := txs[i:end]
		chunkSize := len(chunkTxs)
		chunkElems := make([]rpc.BatchElem, chunkSize)
		chunkReceipts := make([]*types.Receipt, chunkSize)

		for j, tx := range chunkTxs {
			chunkElems[j] = rpc.BatchElem{
				Method: "eth_getTransactionReceipt",
				Args:   []interface{}{tx.Hash()},
				Result: &chunkReceipts[j],
			}
		}

		totalCu := chunkSize * receiptCuPerTx
		if err := p.limiter.WaitN(ctx, totalCu); err != nil {
			logger.Error(ctx, "Rate limiter error in fetchReceiptsBatch",
				err,
				slog.Int("requested_cu", totalCu))
			return nil, err
		}

		err := p.alcEthClient.Client().BatchCallContext(ctx, chunkElems)
		if err != nil {
			logger.Error(ctx, "Failed to fetch receipts batch chunk",
				err,
				slog.String("system", "ethereum"),
				slog.Int("chunk_start", i),
				slog.Int("chunk_size", chunkSize))
			return nil, err
		}

		for j := 0; j < chunkSize; j++ {
			if chunkElems[j].Error != nil {
				logger.Warn(ctx, "Failed to fetch individual tx receipt",
					slog.String("err", chunkElems[j].Error.Error()),
					slog.String("hash", chunkTxs[j].Hash().Hex()))
				continue
			}
			if chunkReceipts[j] != nil {
				receipts = append(receipts, chunkReceipts[j])
			}
		}
	}

	return receipts, nil
}

func (p *Process) fetchBlockReceipts(ctx context.Context, blockNumberHex string) ([]*types.Receipt, error) {
	var receipts []*types.Receipt

	if err := p.limiter.WaitN(ctx, getBlockReceiptCu); err != nil {
		logger.Error(ctx, "Rate limiter error in fetchReceiptsBatch",
			err,
			slog.Int("requested_cu", getBlockReceiptCu))
		return nil, err
	}

	err := p.alcEthClient.Client().CallContext(ctx, &receipts, "eth_getBlockReceipts", blockNumberHex)
	if err != nil {
		logger.Error(ctx, "Failed to fetch block receipts",
			err,
			slog.String("block", blockNumberHex))
		return nil, err
	}

	return receipts, nil
}

func (p *Process) CalculateBlockTxTip(header *types.Header, receipts []*types.Receipt) blockstore.BlockData {
	blockData := blockstore.BlockData{
		Number:   header.Number.Uint64(),
		BaseFee:  header.BaseFee,
		GasLimit: header.GasLimit,
		Txs:      make([]blockstore.TxInfo, 0, len(receipts)),
	}

	for _, receipt := range receipts {
		if receipt == nil {
			continue
		}

		tip, ok := p.gasanalyzer.EffectiveTipFromReceipt(receipt.EffectiveGasPrice, blockData.BaseFee)
		if !ok {
			continue
		}

		weight := p.gasanalyzer.CalculateWeightForGasUsed(receipt.GasUsed, blockData.GasLimit)
		blockData.Txs = append(blockData.Txs, blockstore.TxInfo{
			Hash:      receipt.TxHash.Hex(),
			Tip:       tip,
			GasUsed:   receipt.GasUsed,
			GasWeight: weight,
		})
	}

	return blockData
}

func (p *Process) CompareFeeHistory(ctx context.Context) {
	if err := p.limiter.WaitN(ctx, feeHistoryCu); err != nil {
		logger.Error(ctx, "Rate limiter error in CompareFeeHistory",
			err,
			"system", "analysis",
			"requested_cu", feeHistoryCu)
		return
	}
	p.gasanalyzer.CompareFeeHistory(p.alcEthClient)
}

func (p *Process) ClearMempool(ctx context.Context) {
	p.pendingPool.Clear()
}

func (p *Process) ClearMempoolToTx(ctx context.Context, header *types.Header, receipts []*types.Receipt) {
	if len(receipts) == 0 {
		return
	}

	//sync.Pool에서 슬라이스 메모리 빌리기
	minedHashes := minedHashPool.Get().(map[string]struct{})
	for _, receipt := range receipts {
		minedHashes[receipt.TxHash.Hex()] = struct{}{}
	}

	removedCnt := p.pendingPool.CollectAndClean(minedHashes)

	//모든 데이터 삭제
	for k := range minedHashes {
		delete(minedHashes, k)
	}
	minedHashPool.Put(minedHashes)

	if removedCnt > 0 {
		logger.Info(ctx, "Transactions cleared from mempool",
			slog.Uint64("block_number", header.Number.Uint64()),
			slog.Int("removed_count", removedCnt),
		)
	}
}

func (p *Process) UpdateBlockInfoForAnalysis(header *types.Header) {
	if header.Number == nil {
		return
	}

	currentBlockNumber := header.Number.Uint64()
	nextBaseFee := p.gasanalyzer.CalculateNextBaseFee(header.BaseFee, header.GasUsed, header.GasLimit)
	blockData := p.blockstore.GetBlockData()

	if len(blockData) == 0 {
		return
	}
	pool := make([]gasanalyzer.WeightedTip, 0, len(blockData)*300)

	for i, b := range blockData {
		if i >= len(p.gasanalyzer.DecayTable) {
			break
		}
		decay := p.gasanalyzer.DecayTable[i]

		for _, tx := range b.Txs {
			pool = append(pool, gasanalyzer.WeightedTip{
				Tip:    tx.Tip,
				Weight: tx.GasWeight * decay,
			})
		}
	}

	blockResult := p.gasanalyzer.WeightedPercentiles(pool)

	// 가스 분석을 위한 블록 정보 업데이트
	p.gasanalyzer.UpdateLatestBlockData(
		currentBlockNumber,
		header.BaseFee,
		header.GasUsed,
		header.GasLimit,
		nextBaseFee,
	)
	p.gasanalyzer.UpdateAnalBlockTxPredictionGasResult(blockResult)
}
