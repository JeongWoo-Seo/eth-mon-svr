package gasanalyzer

import (
	"context"
	"log/slog"
	"math"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
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
	gasOracle   *GasOracle
}

func NewAnalyzer(lamda float64, pendingPool *mempool.PendingMemPool, gasOracle *GasOracle) *Analyzer {
	a := &Analyzer{
		pendingPool: pendingPool,
		gasOracle:   gasOracle,
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
	ticker := time.NewTicker(1 * time.Second)
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
	a.mu.RUnlock()

	//pending tx weight 계산
	pendingData := a.collectPendingTx(a.latestBlockData.NextBaseFee, a.latestBlockData.GasLimit)

	//가중 백분위 계산
	peingResult := a.WeightedPercentiles(pendingData)

	//결과 업데이트
	a.UpdateAnalPendingTxPredictionGasResult(peingResult)

	a.UpdateResult(nextBlockNum, nextBaseFee)

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

	result := make(map[string]uint64, len(GasPredictionTargets))
	var cumulativeWeight float64
	targetIdx := 0

	for _, tx := range poolData {
		cumulativeWeight += tx.Weight

		for targetIdx < len(GasPredictionTargets) && cumulativeWeight >= GasPredictionTargets[targetIdx].Ratio*totalWeight {
			result[GasPredictionTargets[targetIdx].Name] = tx.Tip
			targetIdx++
		}

		if targetIdx >= len(GasPredictionTargets) {
			break
		}
	}

	//팁이 남은경우 채우기
	lastTip := poolData[len(poolData)-1].Tip
	for targetIdx < len(GasPredictionTargets) {
		result[GasPredictionTargets[targetIdx].Name] = lastTip
		targetIdx++
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

func (a *Analyzer) UpdateResult(nextBlockNum uint64, nextBaseFee *big.Int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.latestResult.NextBlockNumber = nextBlockNum
	if nextBaseFee != nil {
		a.latestResult.NextBaseFee = nextBaseFee.Uint64()
	}

	a.latestResult.UpdatedAt = time.Now()
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

func (a *Analyzer) UpdateOracleBlockTxPredictionGasResult(result map[string]uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.latestResult.oracleBlock = make(map[string]GasLevel, len(result))

	var baseFee uint64
	if a.latestBlockData.NextBaseFee != nil {
		baseFee = a.latestBlockData.NextBaseFee.Uint64()
	}

	for level, fee := range result {
		a.latestResult.oracleBlock[level] = GasLevel{
			PriorityFee: fee,
			MaxFee:      baseFee + fee,
		}
	}
}

func (a *Analyzer) UpdateOraclePendingTxPredictionGasResult(result map[string]uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.latestResult.oraclePending = make(map[string]GasLevel, len(result))

	var baseFee uint64
	if a.latestBlockData.NextBaseFee != nil {
		baseFee = a.latestBlockData.NextBaseFee.Uint64()
	}

	for level, fee := range result {
		a.latestResult.oraclePending[level] = GasLevel{
			PriorityFee: fee,
			MaxFee:      baseFee + fee,
		}
	}
}
