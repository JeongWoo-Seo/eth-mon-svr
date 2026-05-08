package gasanalyzer

import (
	"math"
	"math/big"
)

func EffectiveTip(feeCap, tipCap, baseFee *big.Int) (uint64, bool) {
	if feeCap == nil || tipCap == nil || baseFee == nil {
		return 0, false
	}

	diff := new(big.Int).Sub(feeCap, baseFee)

	if diff.Sign() <= 0 { //음수 처리
		return 0, false
	}

	// min(tipCap, diff)
	var result *big.Int
	if diff.Cmp(tipCap) > 0 {
		result = tipCap
	} else {
		result = diff
	}

	if !result.IsUint64() { // uint64 범위를 넘는지 확인
		return 0, false
	}

	return result.Uint64(), true
}

func CalculateWeightForGasUsed(gasUsed, gasLimit uint64) float64 {
	if gasLimit == 0 {
		return 0
	}

	return math.Sqrt(float64(gasUsed) / float64(gasLimit))
}
