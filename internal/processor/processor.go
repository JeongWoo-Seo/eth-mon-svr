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
	"golang.org/x/time/rate"
)

const (
	getTxChunkSize = 8  // 한 번에 보낼 최대 RPC 요청 개수
	getTxCuPerTx   = 11 // eth_getTransactionByHash 건당 11 CU 소모
	feeHistoryCu   = 15
	blockByHashCu  = 21
)

type Process struct {
	pendingPool  *mempool.PendingMemPool
	blockstore   *blockstore.Store
	alcEthClient *ethclient.Client
	infEthClient *ethclient.Client
	gasanalyzer  *gasanalyzer.Analyzer

	limiter *rate.Limiter

	mu             sync.RWMutex
	isFallbackMode bool
	fallbackUntil  time.Time
}

func NewProcess(pendingPool *mempool.PendingMemPool, blockstore *blockstore.Store, alcClient *ethclient.Client, infClient *ethclient.Client,
	gasanalyzer *gasanalyzer.Analyzer) *Process {
	return &Process{
		pendingPool:  pendingPool,
		blockstore:   blockstore,
		alcEthClient: alcClient,
		infEthClient: infClient,
		gasanalyzer:  gasanalyzer,

		limiter: rate.NewLimiter(rate.Limit(400), 500),

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

	ctx := context.Background()

	for i := 0; i < len(hashes); i += getTxChunkSize {
		end := i + getTxChunkSize
		if end > len(hashes) {
			end = len(hashes)
		}

		chunkHashes := hashes[i:end]
		chunkSize := len(chunkHashes)
		chunkElems := make([]rpc.BatchElem, chunkSize)
		chunkResults := make([]*types.Transaction, chunkSize)

		for j, hash := range chunkHashes {
			chunkElems[j] = rpc.BatchElem{
				Method: "eth_getTransactionByHash",
				Args:   []interface{}{hash},
				Result: &chunkResults[j],
			}
		}

		totalCu := chunkSize * getTxCuPerTx
		if err := p.limiter.WaitN(ctx, totalCu); err != nil {
			logger.Error(ctx, "Rate limiter error in GetTxInfo",
				err,
				slog.Int("requested_cu", totalCu))
			break
		}

		// 알케미 요청
		err := p.alcEthClient.Client().BatchCallContext(ctx, chunkElems)
		if err != nil {
			logger.Error(ctx, "Failed to get tx info chunk",
				err,
				slog.String("system", "ethereum"),
				slog.Int("chunk_start", i),
				slog.Int("chunk_size", chunkSize))
			continue
		}

		// 개별 에러 검증 및 정상 결과 수집
		validResults := make([]*types.Transaction, 0, chunkSize)
		for j := 0; j < chunkSize; j++ {
			if chunkElems[j].Error != nil {
				logger.Warn(ctx, "Failed to fetch individual tx",
					slog.String("err", chunkElems[j].Error.Error()),
					slog.String("hash", chunkHashes[j].Hex()))
				continue
			}
			if chunkResults[j] != nil {
				validResults = append(validResults, chunkResults[j])
			}
		}

		if len(validResults) > 0 {
			report.IncTxFetched(uint64(len(validResults)))
			p.pendingPool.PushBatch(validResults)
		}
	}
}

func (p *Process) ProcessBlock(header *types.Header) {
	ctx := context.Background()

	// 이전 블록 결과 분석
	p.CompareFeeHistory(ctx)

	logger.Info(ctx, "Create new block",
		slog.String("system", "ethereum"),
		slog.String("block_hash", header.Hash().Hex()))

	// tx 영수증 가져오기
	receipts, err := p.fetchBlockReceipts(ctx, header.Hash().Hex())
	if err != nil {
		logger.Error(ctx, "Failed to fetch receipts batch",
			err,
			slog.String("system", "ethereum"),
			slog.String("block_hash", header.Hash().Hex()),
		)

		p.ClearMempool(ctx)
		return
	}

	if len(receipts) == 0 {
		return
	}

	// 블록 데이터 가공
	blockData := p.CalculateBlockTxTip(header, receipts)

	//데이터 저장
	p.blockstore.AddBlock(blockData)
	p.ClearMempoolToTx(ctx, header, receipts)

	// 분석을 위한 블록 및 tx 정보 업데이트 //각 block에 대한 결과값 계산
	p.UpdateBlockInfoForAnalysis(header)
}
