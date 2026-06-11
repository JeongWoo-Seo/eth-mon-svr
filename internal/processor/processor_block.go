package processor

import (
	"context"
	"log/slog"
	"math/big"
	"slices"
	"sync"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/blockstore"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/gasanalyzer"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

var minedHashPool = sync.Pool{
	New: func() interface{} {
		// 초기 캡시티는 프로젝트의 평균 블록당 트랙잭션 수(예: 200~300개)로 잡아두면 좋습니다.
		return make(map[string]struct{}, 256)
	},
}

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

	err := p.ethClientFunc(ctx, func(client *ethclient.Client) error {
		return client.Client().BatchCallContext(ctx, elems)
	})
	if err != nil {
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
			GasUsed:   receipts[i].GasUsed,
			GasWeight: weight,
		})
	}

	return blockData
}

func (p *Process) ClearMempool(ctx context.Context) {
	p.pendingPool.Clear()
}

func (p *Process) ClearMempoolToTx(ctx context.Context, block *types.Block, txs types.Transactions) {
	if len(txs) == 0 {
		return
	}

	//sync.Pool에서 슬라이스 메모리 빌리기
	minedHashes := minedHashPool.Get().(map[string]struct{})
	for _, tx := range txs {
		minedHashes[tx.Hash().Hex()] = struct{}{}
	}

	removedCnt := p.pendingPool.CollectAndClean(minedHashes)

	//모든 데이터 삭제
	for k := range minedHashes {
		delete(minedHashes, k)
	}
	minedHashPool.Put(minedHashes)

	if removedCnt > 0 {
		logger.Info(ctx, "Transactions cleared from mempool",
			slog.Uint64("block_number", block.NumberU64()),
			slog.Int("removed_count", removedCnt),
		)
	}
}

func (p *Process) UpdateBlockInfoForAnalysis(currentBlockNumber uint64, baseFee *big.Int, gasUsed, gasLimit uint64) {
	nextBaseFee := p.gasanalyzer.CalculateNextBaseFee(baseFee, gasUsed, gasLimit)
	blockData := p.blockstore.GetBlockData()

	if len(blockData) == 0 {
		return
	}
	pool := make([]gasanalyzer.WeightedTip, 0, len(blockData)*200)

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

	//내림차순
	slices.SortFunc(pool, func(a, b gasanalyzer.WeightedTip) int {
		if a.Tip > b.Tip {
			return -1
		}
		if a.Tip < b.Tip {
			return 1
		}
		return 0
	})

	// 가스 분석을 위한 블록 정보 업데이트
	p.gasanalyzer.UpdateLatestBlockData(
		currentBlockNumber,
		baseFee,
		gasUsed,
		gasLimit,
		nextBaseFee,
		pool,
	)
}

func (p *Process) UpdateBlockInfoForAnalysisForHistogram(currentBlockNumber uint64, baseFee *big.Int, gasUsed, gasLimit uint64) {
	nextBaseFee := p.gasanalyzer.CalculateNextBaseFee(baseFee, gasUsed, gasLimit)
	blockData := p.blockstore.GetBlockData()

	if len(blockData) == 0 {
		return
	}
	pool := make([]gasanalyzer.GasTip, 0, len(blockData)*200)

	for _, b := range blockData {
		for _, tx := range b.Txs {
			pool = append(pool, gasanalyzer.GasTip{
				Tip: tx.Tip,
				Gas: tx.GasUsed,
			})
		}
	}

	p.gasOracle.BlockHist.Add(pool)

	p.gasOracle.UpdateLatestBlockData(
		currentBlockNumber,
		baseFee,
		gasUsed,
		gasLimit,
		nextBaseFee,
	)
}
