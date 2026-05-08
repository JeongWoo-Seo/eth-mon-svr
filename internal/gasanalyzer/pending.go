package gasanalyzer

import (
	"math/big"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
)

func PendingTips(state *mempool.State, baseFee *big.Int) []WeightedTip {
	snap := state.Snapshot()

	tips := make([]WeightedTip, 0, len(snap))

	for _, tx := range snap {
		tip, ok := EffectiveTip(tx.GasFeeCap, tx.GasTipCap, baseFee)
		if !ok {
			continue
		}
		tips = append(tips, WeightedTip{
			Tip:    tip,
			Weight: 1.0,
		})
	}

	return tips
}
