package gasanalyzer

import (
	"cmp"
	"context"
	"log/slog"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/network/grpcClient"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"github.com/ethereum/go-ethereum/core/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	MaxAge = 20
)

type Analyzer struct {
	DecayTable      [MaxAge + 1]float64
	mu              sync.RWMutex
	latestResult    GasPrediction
	latestBlockData BlockAnalysisData

	pendingPool *mempool.PendingMemPool
	grpcClient  *grpcClient.GasPredictionClient
}

func NewAnalyzer(lamda float64, pendingPool *mempool.PendingMemPool, grpcClient *grpcClient.GasPredictionClient) *Analyzer {
	a := &Analyzer{
		pendingPool: pendingPool,
		grpcClient:  grpcClient,
	}

	for age := 0; age < MaxAge; age++ {
		a.DecayTable[age] = math.Exp(-lamda * float64(age))
	}

	return a
}

func (a *Analyzer) Start(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	logger.Info(ctx, "Gas analyzer started")
	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "Gas analyzer stopped")
			return
		case <-ticker.C:
			a.AnalyzeGasPrice()
		}
	}
}

func (a *Analyzer) AnalyzeGasPrice() {
	a.mu.RLock()
	nextBlockNum := a.latestBlockData.BlockNumber + 1
	nextBaseFee := a.latestBlockData.NextBaseFee
	baseFee := a.latestBlockData.BaseFee
	currentBaseFee := a.latestBlockData.BaseFee
	gasLimit := a.latestBlockData.GasLimit
	cutoff := a.latestBlockData.Cutoff
	a.mu.RUnlock()

	if nextBlockNum == 1 {
		logger.Warn(context.Background(), "not yet ready to analyze",
			slog.String("system", "analysis"),
		)
		return
	}

	//pending tx weight 계산
	pendingData := a.collectPendingTx(baseFee, nextBaseFee, gasLimit, cutoff)

	//가중 백분위 계산
	pendingResult := a.PendingPercentiles(pendingData)

	//결과 업데이트
	a.UpdateAnalPendingTxPredictionGasResult(pendingResult)

	predictionResult := a.UpdateResult(nextBlockNum, currentBaseFee, nextBaseFee)

	// 결과 web 서버로 전달
	if predictionResult != nil {
		a.sendResultToGRPC(predictionResult)
	}

	logger.Info(context.Background(), "Gas analysis complete",
		slog.String("system", "analysis"),
		slog.Uint64("next block number", nextBlockNum),
	)
}

func (a *Analyzer) collectPendingTx(baseFee, nextBaseFee, blockGasLimit, cutoff uint64) []WeightedTip {
	pendingData := a.pendingPool.Snapshot()
	pool := make([]WeightedTip, 0, len(pendingData))

	for _, tx := range pendingData {
		//Next Block Fee Filtering(FeeCap >= nextBaseFee)
		if tx.FeeCap < nextBaseFee {
			continue
		}

		//min(TipCap, FeeCap-nextBaseFee)
		tip, ok := a.EffectiveTip(tx.FeeCap, tx.TipCap, nextBaseFee)
		if !ok {
			continue
		}

		// //하위 20 cutoff
		// if baseFee != 0 {
		// 	ratio := float64(nextBaseFee) / float64(baseFee)
		// 	if ratio > 1.2 {
		// 		ratio = 1.2
		// 	}
		// 	if ratio < 1.0 {
		// 		ratio = 1.0
		// 	}
		// 	dynamicCutoff := uint64(float64(cutoff) * ratio)
		// 	if tip < dynamicCutoff {
		// 		continue
		// 	}
		// }

		//outer cap -  outer 이상의 tip 값을 outer 값으로 치환

		weight := a.CalculateWeightForGasUsed(tx.GasLimit, blockGasLimit)
		//nonce gap 이면 weight를 0.5 비율로
		if tx.NonceGap {
			weight *= 0.5
		}
		pool = append(pool, WeightedTip{
			Tip:    tip,
			Weight: weight,
		})
	}

	return pool
}

func (a *Analyzer) PendingPercentiles(poolData []WeightedTip) map[string]uint64 {
	if len(poolData) == 0 {
		result, _ := defaultValue()
		logger.Warn(context.Background(), " WeightedPercentiles default value: poolData =0",
			slog.String("sysyem", "analysis"))
		return result
	}

	// Tip 오름차순 정렬
	slices.SortFunc(poolData, func(a, b WeightedTip) int {
		return cmp.Compare(a.Tip, b.Tip)
	})

	// 최대 Tip 확인
	logger.Warn(
		context.Background(),
		"pending tip statistics",
		slog.String("system", "test"),
		slog.Int("txCount", len(poolData)),
		slog.Uint64("maxTip", poolData[len(poolData)-1].Tip),
	)
	//outer 계산
	outer := calculateOuterBound(poolData)

	if outer.Count > 0 {
		start := len(poolData) - outer.Count

		for i := start; i < len(poolData); i++ {
			poolData[i].Tip = outer.UpperBound
		}
	}

	percentiles := calculateWeightedPercentiles(poolData)

	return buildPrediction(percentiles)
}

func (a *Analyzer) BlockPercentiles(poolData []WeightedTip) (map[string]uint64, uint64) {
	if len(poolData) == 0 {
		logger.Warn(context.Background(), " WeightedPercentiles default value: poolData =0",
			slog.String("sysyem", "analysis"))
		return defaultValue()
	}

	// Tip 오름차순 정렬
	slices.SortFunc(poolData, func(a, b WeightedTip) int {
		return cmp.Compare(a.Tip, b.Tip)
	})

	percentiles := calculateWeightedPercentiles(poolData)
	result := buildPrediction(percentiles)

	return result, percentiles[P20]
}

func (a *Analyzer) UpdateResult(nextBlockNum, currentBaseFee, nextBaseFee uint64) *GasPrediction {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.latestResult.NextBlockNumber = nextBlockNum
	a.latestResult.NextBaseFee = nextBaseFee
	if a.latestResult.PredictResult == nil {
		a.latestResult.PredictResult = make(map[string]GasLevel)
	}

	// BaseFee 변화 추세를 기반 가중치
	const sensitivity = 0.6
	multiplier := 1.0
	// current := currentBaseFee
	// next := nextBaseFee
	// rate := float64(next-current) / float64(current)
	// multiplier = 1.0 + (rate * sensitivity)

	// 각 가스 등급별 예측 타겟 연산 및 보정
	for _, t := range GasPredictionTargets {
		if _, ok := a.latestResult.AnalyzerBlock[t.Name]; ok {
			anaBlock := uint64(a.latestResult.AnalyzerBlock[t.Name].PriorityFee)
			anaPending := uint64(a.latestResult.AnalyzerPending[t.Name].PriorityFee)

			blend := float64(anaBlock)*0.2 + float64(anaPending)*0.8
			priorityFee := uint64(blend * multiplier)

			a.latestResult.PredictResult[t.Name] = GasLevel{
				PriorityFee: priorityFee,
				MaxFee:      a.latestResult.NextBaseFee + priorityFee,
			}

		} else {
			logger.Warn(context.Background(), "result data is empty",
				slog.String("sysyem", "analysis"))
		}
	}

	a.latestResult.UpdatedAt = time.Now()

	result := a.latestResult
	return &result
}

func (a *Analyzer) sendResultToGRPC(result *GasPrediction) {
	//pb 형태로 변환
	pbPredictResult := make(map[string]*pb.GasLevel, len(result.PredictResult))
	for k, v := range result.PredictResult {
		pbPredictResult[k] = &pb.GasLevel{
			PriorityFee: v.PriorityFee,
			MaxFee:      v.MaxFee,
		}
	}

	req := &pb.GasPredictionRequest{
		NextBlockNumber: result.NextBlockNumber,
		NextBaseFee:     result.NextBaseFee,
		PredictResult:   pbPredictResult,
		UpdatedAt:       timestamppb.New(result.UpdatedAt),
	}

	a.grpcClient.GasPredictResultSend(req)
}

func (a *Analyzer) GetPrediction() GasPrediction {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.latestResult
}

func (a *Analyzer) UpdateLatestBlockData(header *types.Header, nextBaseFee, cutoff uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.latestBlockData = BlockAnalysisData{
		BlockNumber: header.Number.Uint64(),
		BlockTime:   header.Time,
		BaseFee:     header.BaseFee.Uint64(),
		NextBaseFee: nextBaseFee,
		GasUsed:     header.GasUsed,
		GasLimit:    header.GasLimit,
		UpdatedAt:   time.Now(),
		Cutoff:      cutoff,
	}
}

func (a *Analyzer) UpdateAnalBlockTxPredictionGasResult(result map[string]uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.latestResult.AnalyzerBlock = make(map[string]GasLevel, len(result))
	baseFee := a.latestBlockData.NextBaseFee

	for level, fee := range result {
		a.latestResult.AnalyzerBlock[level] = GasLevel{
			PriorityFee: fee,
			MaxFee:      baseFee + fee,
		}
	}
}

func (a *Analyzer) UpdateAnalPendingTxPredictionGasResult(result map[string]uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.latestResult.AnalyzerPending = make(map[string]GasLevel, len(result))
	baseFee := a.latestBlockData.NextBaseFee

	for level, fee := range result {
		a.latestResult.AnalyzerPending[level] = GasLevel{
			PriorityFee: fee,
			MaxFee:      baseFee + fee,
		}
	}
}

func (a *Analyzer) GetCurrentBlockNumAndTime() (uint64, uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.latestBlockData.BlockNumber, a.latestBlockData.BlockTime
}
