package processor

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/blockstore"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/gasanalyzer"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/network/grpcClient"
	rpcmanager "github.com/JeongWoo-Seo/eth-mon-svr/internal/network/rpcManager"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/dustin/go-humanize"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/time/rate"
)

const (
	getTxChunkSize     = 8  // 한 번에 보낼 최대 RPC 요청 개수
	getTxCuPerTx       = 11 // eth_getTransactionByHash 건당 11 CU 소모
	feeHistoryCu       = 15
	blockByHashCu      = 21
	getBlockTxCountCu  = 20
	getBlockByNumberCu = 16
)

type Process struct {
	pendingPool *mempool.PendingMemPool
	blockstore  *blockstore.Store
	rpcManager  *rpcmanager.RPCManager
	gasanalyzer *gasanalyzer.Analyzer
	grpcClient  *grpcClient.GasPredictionClient

	limiter *rate.Limiter

	mu sync.RWMutex
}

func NewProcess(pendingPool *mempool.PendingMemPool, blockstore *blockstore.Store, rpcManager *rpcmanager.RPCManager,
	gasanalyzer *gasanalyzer.Analyzer, grpcClinet *grpcClient.GasPredictionClient) *Process {
	return &Process{
		pendingPool: pendingPool,
		blockstore:  blockstore,
		rpcManager:  rpcManager,
		gasanalyzer: gasanalyzer,
		grpcClient:  grpcClinet,

		limiter: rate.NewLimiter(rate.Limit(400), 500),
	}
}

func (p *Process) GetTxInfo(ctx context.Context, hashes []common.Hash) {
	if len(hashes) == 0 {
		return
	}

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
				slog.String("system", "limiter"),
				slog.Int("requested_cu", totalCu))
			break
		}

		// tx 정보 요청
		err := p.rpcManager.EthClientFunc(ctx, func(client *ethclient.Client) error {
			return client.Client().BatchCallContext(ctx, chunkElems)
		})
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

func (p *Process) ProcessBlock(ctx context.Context, header *types.Header) error {
	if header == nil {
		return ErrLatestBlockHeaderNil
	}

	if header.Number == nil {
		return fmt.Errorf("%w: hash=%s", ErrLatestBlockNumberNil, header.Hash().Hex())
	}

	logger.Info(ctx, "Create new block",
		slog.String("system", "ethereum"),
		slog.String("block_hash", header.Hash().Hex()))

	blockNumber := header.Number.Uint64()
	// tx 영수증 가져오기
	receipts, err := p.fetchBlockReceipts(ctx, header.Hash().Hex())
	if err != nil {
		return fmt.Errorf("failed to fetch receipts block=%d: %w", blockNumber, err)
	}

	if len(receipts) == 0 {
		logger.Info(ctx, "Empty block",
			slog.String("system", "ethereum"),
			slog.Uint64("block_number", blockNumber),
		)

		// 블록은 진행되었으므로 TTL 만료 처리
		p.removeExpired(blockNumber)
		p.UpdateBlockInfoForAnalysis(header)

		return nil
	}

	// 블록 데이터 가공
	blockData := p.CalculateBlockTxTip(header, receipts)

	// 블록에 포함된 tx 삭제 및 몇 블록만에 블록에 포함되었는지 계산
	blockData.FeeBuckets = p.ClearMempoolToReceipts(ctx, header, receipts)

	//block pool에 저장
	p.blockstore.AddBlock(blockData)

	// 오래된 tx 삭제
	p.removeExpired(blockNumber)

	// 분석을 위한 블록 및 tx 정보 업데이트 //각 block에 대한 결과값 계산
	p.UpdateBlockInfoForAnalysis(header)

	//feebucket grpc 전송
	p.SendFeeBucketsToGrpc()

	return nil
}

func (p *Process) Initialize(ctx context.Context) (*types.Header, error) {
	// 최신 블록 조회
	header, err := p.getLastestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	// receipt 조회
	err = p.initialFromHeader(ctx, header)
	if err != nil {
		return nil, err
	}

	//분석 준비 완료
	p.gasanalyzer.SetReady()

	logger.Info(ctx, "Initialization completed",
		slog.String("system", "ethereum"),
		slog.Uint64("block_number", header.Number.Uint64()),
	)
	return header, nil
}

func (p *Process) HeaderByNumber(ctx context.Context, number uint64) (*types.Header, error) {
	if err := p.limiter.WaitN(ctx, getBlockByNumberCu); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRateLimiterWait, err)
	}

	var header *types.Header

	// tx 정보 요청
	err := p.rpcManager.EthClientFunc(ctx, func(client *ethclient.Client) error {
		var err error
		header, err = client.HeaderByNumber(ctx, new(big.Int).SetUint64(number))
		return err
	})
	if err != nil {
		return nil, err
	}

	return header, nil

}

func (p *Process) CleanupFailedBlock(ctx context.Context, header *types.Header) error {
	if header == nil || header.Number == nil {
		return fmt.Errorf("invalid block header")
	}

	blockNum := header.Number.Uint64()
	receipts, err := p.fetchBlockReceipts(ctx, header.Hash().Hex())
	if err != nil {
		return fmt.Errorf("failed to fetch receipts block=%d: %w", blockNum, err)
	}

	if len(receipts) == 0 {
		logger.Info(ctx, "Empty block",
			slog.String("system", "ethereum"),
			slog.Uint64("block_number", blockNum),
		)
		return nil
	}

	_, removedCnt := p.pendingPool.RemoveByReceipts(header, receipts)

	if removedCnt > 0 {
		logger.Info(ctx, "Transactions cleared from mempool",
			slog.Uint64("block_number", blockNum),
			slog.Int("removed_count", removedCnt),
		)
	}

	return nil
}

func (p *Process) Resync(ctx context.Context) (*types.Header, error) {
	latest, err := p.getLastestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	receipt, err := p.fetchBlockReceipts(ctx, latest.Hash().Hex())
	if err != nil {
		return nil, fmt.Errorf("%w: block=%d hash=%s: %v", ErrBlockReceiptFetch, latest.Number.Uint64(), latest.Hash().Hex(), err)
	}

	if len(receipt) == 0 {
		return nil, fmt.Errorf("%w: block=%d hash=%s", ErrBlockReceiptsEmpty, latest.Number.Uint64(), latest.Hash().Hex())
	}

	// 기존 상태 초기화
	p.gasanalyzer.Reset() //데이터 clear 및 분석 준비 상태 = false
	p.blockstore.Clear()

	//block 데이터 생성
	blockData := p.CalculateBlockTxTip(latest, receipt)

	// blockstore 저장
	p.blockstore.AddBlock(blockData)

	//분석을 위한 데이터 초기화
	p.UpdateBlockInfoForAnalysis(latest)

	//pending pool 초기화
	p.pendingPool.Clear()

	//분석 준비 완료
	p.gasanalyzer.SetReady()

	logger.Info(ctx, "Initialization completed",
		slog.String("system", "ethereum"),
		slog.Uint64("block_number", latest.Number.Uint64()),
	)

	return latest, nil
}

func (p *Process) CompareFeeHistory(ctx context.Context) {
	preResult := p.gasanalyzer.GetPrediction()

	if preResult.NextBlockNumber == 0 || preResult.AnalyzerBlock == nil {
		logger.Info(ctx, "empty result data",
			"system", "analysis",
			"block_num", preResult.NextBlockNumber)
		return
	}

	//0.90 -> 90 으로 변환하기위한 과정
	per := make([]float64, 0, len(gasanalyzer.GasPredictionTargets))
	for _, t := range gasanalyzer.GasPredictionTargets {
		per = append(per, t.Percentile*100)
	}

	if err := p.limiter.WaitN(ctx, feeHistoryCu); err != nil {
		logger.Error(ctx, "Rate limiter error in CompareFeeHistory",
			err,
			"system", "analysis",
			"requested_cu", feeHistoryCu)
		return
	}

	var history *ethereum.FeeHistory
	err := p.rpcManager.EthClientFunc(ctx, func(client *ethclient.Client) error {
		var err error
		history, err = client.FeeHistory(ctx, 1, big.NewInt(int64(preResult.NextBlockNumber)), per)
		return err
	})
	if err != nil {
		logger.Error(ctx, "failed to get block fee history",
			err,
			"system", "analysis",
			"block_num", preResult.NextBlockNumber)
		return
	}

	//결과 비교
	if len(history.Reward) > 0 && len(history.BaseFee) >= 2 {
		reward := history.Reward[0]

		for i, t := range gasanalyzer.GasPredictionTargets {

			actualTip := reward[i].Uint64()

			if _, ok := preResult.AnalyzerBlock[t.Name]; ok {
				anaBlock := int64(preResult.AnalyzerBlock[t.Name].PriorityFee)
				anaPending := int64(preResult.AnalyzerPending[t.Name].PriorityFee)

				blend := int64(preResult.PredictResult[t.Name].PriorityFee)
				diff := blend - int64(actualTip)

				sAnaBlock := humanize.Comma(anaBlock)
				sAnaPending := humanize.Comma(anaPending)
				sBlend := humanize.Comma(blend)
				sActual := humanize.Comma(int64(actualTip))
				sDiff := humanize.Comma(diff)
				if diff > 0 {
					sDiff = "+" + sDiff // 양수일 때 +
				}

				fmt.Printf(
					"%-10s | %-14s | %-14s | %-14s | %-14s | %-12s\n",
					t.Name,
					sAnaBlock,
					sAnaPending,
					sBlend,
					sActual,
					sDiff,
				)

			} else {
				fmt.Printf(
					"%-10s | 데이터 없음\n", t.Name)
			}
		}

		fmt.Printf("BaseFee - 예측 : %d 실제 : %d \n", preResult.NextBaseFee, history.BaseFee[0].Uint64())
	}
}
