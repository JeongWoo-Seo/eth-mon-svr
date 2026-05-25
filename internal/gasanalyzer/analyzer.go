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

var targets = []struct {
	name  string
	ratio float64
}{
	{"low", 0.25},
	{"market", 0.60},
	{"fast", 0.80},
	{"urgent", 0.90},
}

type Analyzer struct {
	DecayTable      [MaxAge + 1]float64
	mu              sync.RWMutex
	latestResult    GasPrediction
	latestBlockData BlockAnalysisData

	pendingPool *mempool.PendingMemPool
}

func NewAnalyzer(lamda float64, pendingPool *mempool.PendingMemPool) *Analyzer {
	a := &Analyzer{
		pendingPool: pendingPool,
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

	//블록의 팁풀 복사
	historyPool := make([]WeightedTip, len(a.latestBlockData.TipPool))
	copy(historyPool, a.latestBlockData.TipPool)
	a.mu.RUnlock()

	//pending tx weight 계산
	pendingData := a.collectPendingTx(a.latestBlockData.NextBaseFee, a.latestBlockData.GasLimit)

	// tx 정보 결합 // 정렬 방법 변경필요
	combinedPool := make([]WeightedTip, 0, len(historyPool)+len(pendingData))
	combinedPool = append(combinedPool, historyPool...)
	combinedPool = append(combinedPool, pendingData...)

	//가중 백분위 계산
	result := a.WeightedPercentiles(combinedPool)

	//결과 업데이트
	a.UpdateResult(nextBlockNum, nextBaseFee, result)

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
			Weight: weight * 0.6,
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

	result := make(map[string]uint64, len(targets))
	var cumulativeWeight float64
	targetIdx := 0

	for _, tx := range poolData {
		cumulativeWeight += tx.Weight

		for targetIdx < len(targets) && cumulativeWeight >= targets[targetIdx].ratio*totalWeight {
			result[targets[targetIdx].name] = tx.Tip
			targetIdx++
		}

		if targetIdx >= len(targets) {
			break
		}
	}

	//팁이 남은경우 채우기
	lastTip := poolData[len(poolData)-1].Tip
	for targetIdx < len(targets) {
		result[targets[targetIdx].name] = lastTip
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

func (a *Analyzer) UpdateResult(nextBlockNum uint64, nextBaseFee *big.Int, result map[string]uint64) {
	u64NextBaseFee := nextBaseFee.Uint64()
	levels := make(map[string]GasLevel)
	for p, r := range result {
		levels[p] = GasLevel{
			PriorityFee: r,
			MaxFee:      u64NextBaseFee + r,
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.latestResult = GasPrediction{
		NextBlockNumber: nextBlockNum,
		NextBaseFee:     u64NextBaseFee,
		Levels:          levels,
		UpdatedAt:       time.Now(),
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
	pool []WeightedTip,
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
		TipPool:     pool,
		UpdatedAt:   time.Now(),
	}
}
