package gasanalyzer

import (
	"math"
	"math/big"
	"sort"
	"sync"
	"time"
)

const (
	MaxAge = 20
)

type Analyzer struct {
	DecayTable   [MaxAge + 1]float64
	mu           sync.RWMutex
	latestResult GasPrediction
}

func NewAnalyzer(lamda float64) *Analyzer {
	a := &Analyzer{}

	for age := 0; age < MaxAge; age++ {
		a.DecayTable[age] = math.Exp(-lamda * float64(age))
	}

	return a
}

func (a *Analyzer) WeightedPercentiles(poolData []WeightedTip) map[string]uint64 {
	if len(poolData) == 0 {
		return a.defaultValue()
	}

	// 정렬
	sort.Slice(poolData, func(i, j int) bool {
		return poolData[i].Tip < poolData[j].Tip
	})

	// 전체 weight 합
	var totalWeight float64
	for _, tip := range poolData {
		totalWeight += tip.Weight
	}

	result := make(map[string]uint64)
	var sum float64
	targets := []struct {
		p     string
		value float64
	}{
		{"P25", 0.25},
		{"P50", 0.50},
		{"P75", 0.75},
		{"P95", 0.95},
	}
	nextTarget := 0

	for _, tx := range poolData {
		sum += tx.Weight
		for nextTarget < len(targets) && sum >= targets[nextTarget].value*totalWeight {
			result[targets[nextTarget].p] = tx.Tip
			nextTarget++
		}

		if nextTarget >= len(targets) {
			break
		}
	}

	//result nexttarget의 값이 남은 경우 pooldata의 마지막 값으로 채움
	for nextTarget < len(targets) {
		result[targets[nextTarget].p] = poolData[len(poolData)-1].Tip
		nextTarget++
	}

	return result
}

func (a *Analyzer) defaultValue() map[string]uint64 {
	return map[string]uint64{
		"P25": 1_000_000_000, // 1 gwei
		"P50": 1_500_000_000, // 1.5 gwei
		"P75": 2_000_000_000, // 2 gwei
		"P95": 3_000_000_000, // 3 gwei
	}
}

func (a *Analyzer) ResultUpdate(blockNum uint64, nextBaseFee *big.Int, result map[string]uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	u64NextBaseFee := nextBaseFee.Uint64()
	levels := make(map[string]GasLevel)
	for p, r := range result {

		levels[p] = GasLevel{
			PriorityFee: r,
			MaxFee:      u64NextBaseFee + r,
		}
	}

	a.latestResult = GasPrediction{
		BlockNumber: blockNum,
		NextBaseFee: u64NextBaseFee,
		Levels:      levels,
		UpdatedAt:   time.Now(),
	}
}
