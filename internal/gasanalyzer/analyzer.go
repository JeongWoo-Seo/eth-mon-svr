package gasanalyzer

import (
	"math"
	"math/big"
	"sort"
	"sync"
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

	sort.Slice(poolData, func(i, j int) bool {
		return poolData[i].Tip < poolData[j].Tip
	})

	var totalWeight float64
	for _, tip := range poolData {
		totalWeight += tip.Weight
	}

	return nil
}

func (a *Analyzer) defaultValue() map[string]uint64 {
	return map[string]uint64{
		"low":      1_000_000_000, // 1 gwei
		"standard": 1_500_000_000, // 1.5 gwei
		"fast":     2_000_000_000, // 2 gwei
		"instant":  3_000_000_000, // 3 gwei
	}
}

func (a *Analyzer) ResultUpdate(blockNum uint64, nextBaseFee *big.Int, result map[string]uint64) {

}
