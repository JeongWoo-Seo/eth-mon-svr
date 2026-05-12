package processor

import (
	"math/big"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/gasanalyzer"
)

func (p *Process) collectBlockTx() []gasanalyzer.WeightedTip {
	blockData := p.blockstore.GetBlockData()
	pool := make([]gasanalyzer.WeightedTip, 0, len(blockData)*200)

	for i, b := range blockData {
		for _, tx := range b.Txs {
			pool = append(pool, gasanalyzer.WeightedTip{
				Tip:    tx.Tip,
				Weight: tx.GasWeight * p.gasanalyzer.DecayTable[i],
			})
		}
	}

	return pool
}

func (p *Process) collectPendingTx(nextBaseFee *big.Int, gasLimit uint64) []gasanalyzer.WeightedTip {
	pendingData := p.state.Snapshot()
	pool := make([]gasanalyzer.WeightedTip, 0, len(pendingData))

	for _, tx := range pendingData {
		tip, ok := p.gasanalyzer.EffectiveTip(tx.GasFeeCap, tx.GasTipCap, nextBaseFee)
		if !ok {
			continue
		}

		weight := p.gasanalyzer.CalculateWeightForGasUsed(tx.Gas, gasLimit)

		pool = append(pool, gasanalyzer.WeightedTip{
			Tip:    tip,
			Weight: weight,
		})
	}

	return pool
}
