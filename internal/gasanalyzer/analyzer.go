package gasanalyzer

import (
	"math"
	"math/big"
	"slices"
	"sync"
	"time"
)

const (
	MaxAge = 20
)

var targets = []struct {
	name  string
	ratio float64
}{
	{"low", 0.25},
	{"market", 0.50},
	{"fast", 0.75},
	{"urgent", 0.90},
}

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

func (a *Analyzer) defaultValue() map[string]uint64 {
	return map[string]uint64{
		"low":    1_000_000_000, // Base + 1 Gwei
		"market": 1_500_000_000, // Base + 1.5 Gwei
		"fast":   2_000_000_000, // Base + 2 Gwei
		"urgent": 5_000_000_000, // Base + 5 Gwei
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
