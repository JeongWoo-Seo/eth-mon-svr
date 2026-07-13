package gasanalyzer

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
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
	grpcClient  pb.GasPredictionServiceClient
}

func NewAnalyzer(lamda float64, pendingPool *mempool.PendingMemPool, grpcClient pb.GasPredictionServiceClient) *Analyzer {
	a := &Analyzer{
		pendingPool: pendingPool,
		grpcClient:  grpcClient,
	}

	for age := 0; age < MaxAge; age++ {
		a.DecayTable[age] = math.Exp(-lamda * float64(age))
	}

	//초기 기본값 설정
	a.latestBlockData.NextBaseFee = new(big.Int)
	a.latestBlockData.BaseFee = new(big.Int)

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
	nextBaseFee := new(big.Int).Set(a.latestBlockData.NextBaseFee)
	currentBaseFee := new(big.Int).Set(a.latestBlockData.BaseFee)
	a.mu.RUnlock()

	//pending tx weight 계산
	pendingData := a.collectPendingTx(a.latestBlockData.NextBaseFee, a.latestBlockData.GasLimit)

	//가중 백분위 계산
	pendingResult := a.WeightedPercentiles(pendingData)

	//결과 업데이트
	a.UpdateAnalPendingTxPredictionGasResult(pendingResult)

	a.UpdateResult(nextBlockNum, currentBaseFee, nextBaseFee)

	logger.Info(context.Background(), "Gas analysis complete",
		slog.String("system", "analysis"),
		slog.Uint64("next block number", nextBlockNum),
	)
}

func (a *Analyzer) collectPendingTx(nextBaseFee *big.Int, gasLimit uint64) []WeightedTip {
	pendingData := a.pendingPool.Snapshot()
	pool := make([]WeightedTip, 0, len(pendingData))

	for _, tx := range pendingData {
		tip, ok := a.EffectiveTip(tx.GasFeeCap, tx.GasTipCap, nextBaseFee)
		if !ok {
			continue
		}

		weight := a.CalculateWeightForGasUsed(tx.Gas, gasLimit)

		pool = append(pool, WeightedTip{
			Tip:    tip,
			Weight: weight,
		})
	}

	return pool
}

func (a *Analyzer) WeightedPercentiles(poolData []WeightedTip) map[string]uint64 {
	if len(poolData) == 0 {
		return defaultValue()
	}

	// 정렬
	slices.SortFunc(poolData, func(a, b WeightedTip) int {
		if a.Tip < b.Tip {
			return -1
		}
		if a.Tip > b.Tip {
			return 1
		}
		return 0
	})

	// 전체 weight 합
	var totalWeight float64
	for _, tip := range poolData {
		totalWeight += tip.Weight
	}

	analysisResults := make([]float64, len(GasAnalysisTargets))
	var cumulativeWeight float64
	analysisIdx := 0

	for _, tx := range poolData {
		cumulativeWeight += tx.Weight

		for analysisIdx < len(GasAnalysisTargets) && cumulativeWeight >= GasAnalysisTargets[analysisIdx]*totalWeight {
			analysisResults[analysisIdx] = float64(tx.Tip)
			analysisIdx++
		}

		if analysisIdx >= len(GasAnalysisTargets) {
			break
		}
	}

	//팁이 남은경우 채우기
	lastTip := float64(poolData[len(poolData)-1].Tip)
	for analysisIdx < len(GasAnalysisTargets) {
		analysisResults[analysisIdx] = lastTip
		analysisIdx++
	}

	//P50,P75,P90
	result := make(map[string]uint64, len(GasPredictionTargets))

	for _, target := range GasPredictionTargets {
		groupKey := target.Name
		if groupKey == "market" {
			groupKey = "p50"
		}
		if groupKey == "fast" {
			groupKey = "p75"
		}
		if groupKey == "urgent" {
			groupKey = "p90"
		}

		group, exist := PredictionGroups[groupKey]

		// 그룹 설정이 없거나 비어있다면, 단일 값으로
		if !exist || len(group) == 0 {
			// 0.40부터 0.05 단위이므로 안전하게 반올림하여 인덱스 계산
			idx := int(((target.Ratio - 0.40) / 0.05) + 0.5)
			if idx >= 0 && idx < len(analysisResults) {
				result[target.Name] = uint64(analysisResults[idx] + 0.5)
			} else {
				result[target.Name] = 0
			}
			continue
		}

		var sumTips float64
		var sumWeights float64
		for _, wp := range group {
			// 인덱스 바운드 체크
			if wp.Index >= 0 && wp.Index < len(analysisResults) {
				sumTips += analysisResults[wp.Index] * wp.Weight
				sumWeights += wp.Weight
			}
		}

		// 2. 최종 가중치 계산 및 반올림 처리
		if sumWeights > 0 {
			finalTip := sumTips / sumWeights

			// [도메인 보정 로직 추가 가능 기점]
			// 예: urgent의 경우 하방 평균으로 인해 값이 낮아지므로 원래 P90 값보다 낮아지지 않도록 방어선 구축
			if groupKey == "p90" && finalTip < analysisResults[P90] {
				// 가중 평균이 실제 P90보다 낮다면 안전을 위해 실제 P90 값을 선택하거나 패널티 상향
				finalTip = analysisResults[P90]
			}

			result[target.Name] = uint64(finalTip + 0.5)
		} else {
			result[target.Name] = 0
		}
	}

	return result
}

func defaultValue() map[string]uint64 {
	return map[string]uint64{
		"low":    1_000_000_000, // Base + 1 Gwei
		"market": 1_500_000_000, // Base + 1.5 Gwei
		"fast":   2_000_000_000, // Base + 2 Gwei
		"urgent": 5_000_000_000, // Base + 5 Gwei
	}
}

func (a *Analyzer) UpdateResult(nextBlockNum uint64, currentBaseFee, nextBaseFee *big.Int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.latestResult.NextBlockNumber = nextBlockNum
	if nextBaseFee != nil {
		a.latestResult.NextBaseFee = nextBaseFee.Uint64()
	}
	if a.latestResult.predictResult == nil {
		a.latestResult.predictResult = make(map[string]GasLevel)
	}

	// BaseFee 변화 추세를 기반 가중치
	const sensitivity = 1.2
	multiplier := 1.0

	if currentBaseFee != nil && nextBaseFee != nil && currentBaseFee.Sign() > 0 {
		current := currentBaseFee.Int64()
		next := nextBaseFee.Int64()
		rate := float64(next-current) / float64(current)
		multiplier = 1.0 + (rate * sensitivity)
	}

	// 각 가스 등급별 예측 타겟 연산 및 보정
	for _, t := range GasPredictionTargets {
		if _, ok := a.latestResult.analyzerBlock[t.Name]; ok {
			anaBlock := uint64(a.latestResult.analyzerBlock[t.Name].PriorityFee)
			anaPending := uint64(a.latestResult.analyzerPending[t.Name].PriorityFee)

			blend := float64(anaBlock)*0.2 + float64(anaPending)*0.8
			priorityFee := uint64(blend * multiplier)

			// 3-3. 하한선 및 음수 방지 예외 처리 (특히 low 등급 방어)
			var minLimit uint64 = 1440000 // 체인별 최저 PriorityFee 하한선 설정
			if t.Name == "low" && priorityFee < minLimit {
				priorityFee = minLimit
			} else if priorityFee < 0 {
				priorityFee = 0
			}

			a.latestResult.predictResult[t.Name] = GasLevel{
				PriorityFee: priorityFee,
				MaxFee:      a.latestResult.NextBaseFee + priorityFee,
			}

			// sAnaBlock := humanize.Comma(int64(anaBlock))
			// sAnaPending := humanize.Comma(int64(anaPending))
			// sFinalFee := humanize.Comma(int64(priorityFee))
			// fmt.Printf("%-10s | %-14s | %-14s \n", t.Name, sAnaBlock, sAnaPending, sFinalFee, )

		} else {
			fmt.Printf("%-10s | 데이터 없음\n", t.Name)
		}
	}

	a.latestResult.UpdatedAt = time.Now()
}

func (a *Analyzer) SendGasPrediction() {
	a.mu.Lock()
	result := a.latestResult
	a.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	gasResult := make(map[string]*pb.GasLevel, len(result.predictResult))
	for k, v := range result.predictResult {
		gasResult[k] = &pb.GasLevel{
			PriorityFee: v.PriorityFee,
			MaxFee:      v.MaxFee,
		}
	}

	req := &pb.GasPredictionRequest{
		NextBlockNumber: result.NextBlockNumber,
		NextBaseFee:     result.NextBaseFee,
		PredictResult:   gasResult,
		UpdatedAt:       timestamppb.New(result.UpdatedAt),
	}

	res, err := a.grpcClient.SendGasPrediction(ctx, req)
	if err != nil {
		logger.Error(context.Background(), "failed to send gRPC prediction result",
			err,
			slog.String("system", "grpc"),
		)
		return
	}

	if !res.Success {
		logger.Warn(context.Background(), "web server reject gas result",
			slog.String("system", "web server"),
			slog.String("message", res.Message),
		)
	}
}

func (a *Analyzer) GetPrediction() GasPrediction {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.latestResult
}

func (a *Analyzer) UpdateLatestBlockData(
	blockNumber uint64,
	baseFee *big.Int,
	gasUsed, gasLimit uint64,
	nextBaseFee *big.Int,
) {
	a.mu.Lock()
	defer a.mu.Unlock()

	//deap copy
	tmBaseFee := new(big.Int).Set(baseFee)
	tmNextBaseFee := new(big.Int).Set(nextBaseFee)

	a.latestBlockData = BlockAnalysisData{
		BlockNumber: blockNumber,
		BaseFee:     tmBaseFee,
		NextBaseFee: tmNextBaseFee,
		GasUsed:     gasUsed,
		GasLimit:    gasLimit,
		UpdatedAt:   time.Now(),
	}
}

func (a *Analyzer) UpdateAnalBlockTxPredictionGasResult(result map[string]uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.latestResult.analyzerBlock = make(map[string]GasLevel, len(result))

	var baseFee uint64
	if a.latestBlockData.NextBaseFee != nil {
		baseFee = a.latestBlockData.NextBaseFee.Uint64()
	}

	for level, fee := range result {
		a.latestResult.analyzerBlock[level] = GasLevel{
			PriorityFee: fee,
			MaxFee:      baseFee + fee,
		}
	}
}

func (a *Analyzer) UpdateAnalPendingTxPredictionGasResult(result map[string]uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.latestResult.analyzerPending = make(map[string]GasLevel, len(result))

	var baseFee uint64
	if a.latestBlockData.NextBaseFee != nil {
		baseFee = a.latestBlockData.NextBaseFee.Uint64()
	}

	for level, fee := range result {
		a.latestResult.analyzerPending[level] = GasLevel{
			PriorityFee: fee,
			MaxFee:      baseFee + fee,
		}
	}
}
