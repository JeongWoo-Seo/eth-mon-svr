package gasanalyzer

import (
	"math"
	"math/big"
	"sort"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	MaxAge = 20
)

type WeightedTip struct {
	Tip    uint64
	Weight float64
}

type Analyzer struct {
	blocks     []*types.Block
	state      *mempool.State
	decayTable [MaxAge + 1]float64
}

func NewAnalyzer(state *mempool.State, lamda float64) *Analyzer {
	a := &Analyzer{
		state: state,
	}

	for age := 0; age < MaxAge; age++ {
		a.decayTable[age] = math.Exp(-lamda * float64(age))
	}

	return a
}

func (a *Analyzer) Suggest(baseFee *big.Int) map[string]uint64 {
	var blockTips [][]WeightedTip

	for i, b := range a.blocks {
		bt := BlockTips(b, i)
		blockTips = append(blockTips, bt)
	}

	pendingTips := PendingTips(a.state, baseFee)

	merged := mergeWeight(blockTips, pendingTips)

	return weightedPercentiles(merged)
}

func weightedPercentiles(tips []WeightedTip) map[string]uint64 {
	if len(tips) == 0 {
		return defaultValue()
	}

	sort.Slice(tips, func(i, j int) bool {
		return tips[i].Tip < tips[j].Tip
	})

	var totalWeight float64
	for _, tip := range tips {
		totalWeight += tip.Weight
	}

	setValue := func(p float64) uint64 {
		target := totalWeight * p
		var cur float64
		for _, tip := range tips {
			cur += tip.Weight
			if target <= cur {
				return tip.Tip
			}
		}

		return tips[len(tips)-1].Tip
	}

	return map[string]uint64{
		"P60": setValue(0.60),
		"P70": setValue(0.70),
		"P80": setValue(0.80),
		"P90": setValue(0.90),
		"P95": setValue(0.95),
	}
}

func defaultValue() map[string]uint64 {
	return map[string]uint64{
		"P60": 2e9,
		"P70": 2e9,
		"P80": 2e9,
		"P90": 2e9,
		"P95": 2e9,
	}
}

func mergeWeight(blockTips [][]WeightedTip, pendingTips []WeightedTip) []WeightedTip {
	total := len(pendingTips)

	for _, b := range blockTips {
		total += len(b)
	}

	out := make([]WeightedTip, 0, total)

	for _, b := range blockTips {
		out = append(out, b...)
	}

	out = append(out, pendingTips...)

	return out
}
