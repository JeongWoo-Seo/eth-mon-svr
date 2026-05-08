package gasanalyzer

import "github.com/ethereum/go-ethereum/core/types"

func BlockTips(block *types.Block, blockIdx int) []WeightedTip {
	baseFee := block.BaseFee()

	baseFee = block.BaseFee()
	//blockGaseUsed = block.GasUsed()

	bw := BlockWeight(blockIdx)

	var result []WeightedTip

	for _, tx := range block.Transactions() {
		if tx.Type() == types.DynamicFeeTxType {
			continue
		}

		tip, ok := EffectiveTip(tx.GasFeeCap(), tx.GasTipCap(), baseFee)
		if !ok {
			continue
		}

		// ⚠️ tx gasUsed는 receipt 필요
		// 여기선 fallback으로 동일 weight 사용 (또는 추후 receipt 연동)
		tw := 1.0 / float64(len(block.Transactions()))

		result = append(result, WeightedTip{
			Tip:    tip,
			Weight: bw * tw,
		})
	}

	return result
}

func BlockWeight(blockIdx int) float64 {
	return float64(20 - blockIdx)
}

func txWeight(txGasUsed, blockGasUsed uint64) float64 {
	if blockGasUsed == 0 {
		return 0
	}

	return float64(txGasUsed) / float64(blockGasUsed)
}
