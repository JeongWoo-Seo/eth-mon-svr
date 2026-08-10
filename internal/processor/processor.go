package processor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/blockstore"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/gasanalyzer"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/grpcClient"
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
	grpcClient   *grpcClient.GasPredictionClient

	limiter *rate.Limiter

	mu             sync.RWMutex
	isFallbackMode bool
	fallbackUntil  time.Time
}

func NewProcess(pendingPool *mempool.PendingMemPool, blockstore *blockstore.Store, alcClient *ethclient.Client, infClient *ethclient.Client,
	gasanalyzer *gasanalyzer.Analyzer, grpcClinet *grpcClient.GasPredictionClient) *Process {
	return &Process{
		pendingPool:  pendingPool,
		blockstore:   blockstore,
		alcEthClient: alcClient,
		infEthClient: infClient,
		gasanalyzer:  gasanalyzer,
		grpcClient:   grpcClinet,

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
			blockNum, blockTime := p.gasanalyzer.GetCurrentBlockNumAndTime()
			p.pendingPool.PushBatch(validResults, blockNum, blockTime)
		}
	}
}

func (p *Process) ProcessBlock(header *types.Header) {
	ctx := context.Background()

	if header == nil {
		logger.Warn(ctx, "Received nil block header",
			slog.String("system", "ethereum"),
		)
		return
	}

	if header.Number == nil {
		logger.Warn(ctx, "Received block header with nil number",
			slog.String("system", "ethereum"),
			slog.String("block_hash", header.Hash().Hex()),
		)
		return
	}

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

		// retry queue 추가

		return
	}

	if len(receipts) == 0 {
		return
	}

	// 블록 데이터 가공
	blockData := p.CalculateBlockTxTip(header, receipts)

	// 블록에 포함된 tx 삭제 및 몇 블록만에 블록에 포함되었는지 계산
	blockData.FeeBuckets = p.ClearMempoolToReceipts(ctx, header, receipts)

	//block pool에 저장
	p.blockstore.AddBlock(blockData)

	// 오래된 tx 삭제
	p.removeExpired(header.Number.Uint64())

	// 분석을 위한 블록 및 tx 정보 업데이트 //각 block에 대한 결과값 계산
	p.UpdateBlockInfoForAnalysis(header)

	//feebucket grpc 전송
	p.SendFeeBucketsToGrpc()
}

func (p *Process) Initialize(ctx context.Context) error {
	// 최신 블록 조회
	header, err := p.alcEthClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get latest block number: %w", err)
	}

	if header == nil {
		return fmt.Errorf("Received nil block header")
	}

	if header.Number == nil {
		return fmt.Errorf("Received block header with nil number")
	}

	// receipt 조회
	receipt, err := p.fetchBlockReceipts(ctx, header.Hash().Hex())
	if err != nil {
		return fmt.Errorf("failed to fetch receipt: %w", err)
	}

	if len(receipt) == 0 {
		return fmt.Errorf("latest block has no receipts")
	}

	//block 데이터 생성
	blockData := p.CalculateBlockTxTip(header, receipt)

	// blockstore 저장
	p.blockstore.AddBlock(blockData)

	//분석을 위한 데이터 초기화
	p.UpdateBlockInfoForAnalysis(header)

	logger.Info(ctx, "Initialization completed",
		slog.String("system", "ethereum"),
		slog.Uint64("block_number", header.Number.Uint64()),
	)
	return nil
}
