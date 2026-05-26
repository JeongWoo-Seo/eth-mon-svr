package processor

import (
	"context"
	"log/slog"
	"sync"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/blockstore"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/gasanalyzer"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type Process struct {
	pendingPool *mempool.PendingMemPool
	blockstore  *blockstore.Store
	ethClient   *ethclient.Client
	gasanalyzer *gasanalyzer.Analyzer
}

var batchElemPool = sync.Pool{
	New: func() interface{} {
		e := make([]rpc.BatchElem, 0, 100)
		return &e
	},
}

var txResultPool = sync.Pool{
	New: func() interface{} {
		r := make([]*types.Transaction, 0, 100)
		return &r
	},
}

func NewProcess(pendingPool *mempool.PendingMemPool, blockstore *blockstore.Store, client *ethclient.Client, gasanalyzer *gasanalyzer.Analyzer) *Process {
	return &Process{
		pendingPool: pendingPool,
		blockstore:  blockstore,
		ethClient:   client,
		gasanalyzer: gasanalyzer,
	}
}

func (p *Process) GetTxInfo(hashes []common.Hash) {
	if len(hashes) == 0 {
		return
	}

	ePtr := batchElemPool.Get().(*[]rpc.BatchElem)
	rPtr := txResultPool.Get().(*[]*types.Transaction)
	elems := (*ePtr)[:0]
	results := (*rPtr)[:0]

	if cap(elems) < len(hashes) {
		elems = make([]rpc.BatchElem, len(hashes))
		results = make([]*types.Transaction, len(hashes))
	} else {
		elems = elems[:len(hashes)]
		results = results[:len(hashes)]
	}

	for i, hash := range hashes {
		results[i] = nil // 이전 결과 초기화
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

	report.IncTxFetched(uint64(len(results)))
	p.pendingPool.PushBatch(results)

	for i := range results {
		results[i] = nil
	}

	*ePtr = elems
	*rPtr = results
	batchElemPool.Put(ePtr)
	txResultPool.Put(rPtr)
}

func (p *Process) ProcessBlock(header *types.Header) {
	ctx := context.Background()

	// 블록 데이터 가져오기
	block, err := p.ethClient.BlockByHash(ctx, header.Hash())
	if err != nil {
		logger.Error(ctx, "Failed to fetch block by hash",
			err,
			slog.String("system", "ethereum"),
			slog.String("block_hash", header.Hash().Hex()))
		return
	}

	logger.Info(ctx, "Create new block",
		slog.String("system", "ethereum"),
		slog.String("block_hash", header.Hash().Hex()))

	//이전 블록 결과 비교
	p.gasanalyzer.CompareFeeHistory(p.ethClient)

	txs := block.Transactions()
	if len(txs) == 0 {
		return
	}

	// tx 영수증 가져오기
	receipt, err := p.fetchReceiptsBatch(ctx, txs)
	if err != nil {
		logger.Error(ctx, "Failed to fetch receipts batch",
			err,
			slog.String("system", "ethereum"),
		)
		// 영수증은 실패했어도 블록에 포함된건 확실하므로 멤풀은 정리
		p.ClearMempool(ctx, block, txs)
		return
	}

	// 블록 데이터 가공
	blockData := p.CalculateBlockTxTip(block, txs, receipt)

	//데이터 저장
	p.blockstore.AddBlock(blockData)
	p.ClearMempool(ctx, block, txs)

	// 분석을 위한 블록 및 tx 정보 업데이트
	p.UpdateBlockInfoForAnalysis(block.NumberU64(), block.BaseFee(), block.GasUsed(), block.GasLimit())
}
