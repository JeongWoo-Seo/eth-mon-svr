package processor

import (
	"context"
	"log/slog"
	"sync"
	"time"

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
	pendingPool  *mempool.PendingMemPool
	blockstore   *blockstore.Store
	alcEthClient *ethclient.Client
	infEthClient *ethclient.Client
	gasanalyzer  *gasanalyzer.Analyzer
	gasOracle    *gasanalyzer.GasOracle

	mu             sync.RWMutex
	isFallbackMode bool
	fallbackUntil  time.Time
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

func NewProcess(pendingPool *mempool.PendingMemPool, blockstore *blockstore.Store, alcClient *ethclient.Client, infClient *ethclient.Client,
	gasanalyzer *gasanalyzer.Analyzer, gasOracle *gasanalyzer.GasOracle) *Process {
	return &Process{
		pendingPool:  pendingPool,
		blockstore:   blockstore,
		alcEthClient: alcClient,
		infEthClient: infClient,
		gasanalyzer:  gasanalyzer,
		gasOracle:    gasOracle,

		isFallbackMode: false,
		fallbackUntil:  time.Now(),
	}
}

func (p *Process) ethClientFunc(ctx context.Context, fn func(client *ethclient.Client) error) error {
	now := time.Now()
	p.mu.Lock()
	if p.isFallbackMode && now.After(p.fallbackUntil) {
		p.isFallbackMode = false
	}
	isFallbackMode := p.isFallbackMode && now.Before(p.fallbackUntil)
	p.mu.Unlock()

	//인프라 백업 클라이언트로 실행
	if isFallbackMode {
		return fn(p.infEthClient)
	}

	// 알케미 클라이언트 실행
	if err := fn(p.alcEthClient); err == nil {
		return nil
	}

	p.mu.Lock()
	if !p.isFallbackMode {
		p.isFallbackMode = true
		p.fallbackUntil = time.Now().Add(1 * time.Minute)
	}
	p.mu.Unlock()

	//알케미 실패시 인프라로 다시 실행
	return fn(p.infEthClient)
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
	err := p.alcEthClient.Client().BatchCallContext(ctx, elems)
	if err != nil {
		logger.Error(ctx, "Failed to get tx info",
			err,
			slog.String("system", "ethereum"),
			slog.Int("tx size", len(elems)))
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

	//retry 코드 추가 필요
	block, err := p.alcEthClient.BlockByHash(ctx, header.Hash())
	if err != nil {
		logger.Error(ctx, "Failed to fetch block by hash",
			err,
			slog.String("system", "ethereum"),
			slog.String("block_hash", header.Hash().Hex()))

		// pending 데이터 삭제 - 오류 발생시 mempool에 tx가 계속 남아 있는 현상이 발생하여 오류발생시 모든 tx를 삭제
		p.ClearMempool(ctx)
		return
	}

	logger.Info(ctx, "Create new block",
		slog.String("system", "ethereum"),
		slog.String("block_hash", header.Hash().Hex()))

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
		p.ClearMempoolToTx(ctx, block, txs)
		return
	}

	// 블록 데이터 가공
	blockData := p.CalculateBlockTxTip(block, txs, receipt)

	//데이터 저장
	p.blockstore.AddBlock(blockData)
	p.ClearMempoolToTx(ctx, block, txs)

	//이전 블록 결과 비교
	p.gasanalyzer.CompareFeeHistory(p.alcEthClient)

	// 분석을 위한 블록 및 tx 정보 업데이트 //각 block에 대한 결과값 계산
	p.UpdateBlockInfoForAnalysis(block.NumberU64(), block.BaseFee(), block.GasUsed(), block.GasLimit())
	p.UpdateBlockInfoForAnalysisForHistogram(block.NumberU64(), block.BaseFee(), block.GasUsed(), block.GasLimit())
}
