package gasanalyzer

import (
	"math"
	"math/big"
)

func (a *Analyzer) EffectiveTip(feeCap, tipCap, baseFee *big.Int) (uint64, bool) {
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

func (a *Analyzer) EffectiveTipFromReceipt(effectiveGasPrice, baseFee *big.Int) (uint64, bool) {
	if effectiveGasPrice == nil || baseFee == nil {
		return 0, false
	}

	// EffectiveTip = EffectiveGasPrice - BaseFee
	tip := new(big.Int).Sub(effectiveGasPrice, baseFee)

	// 음수 처리 (혹시 모를 예외 상황 방지)
	if tip.Sign() <= 0 {
		return 0, false
	}

	if !tip.IsUint64() {
		return 0, false
	}

	return tip.Uint64(), true
}

func (a *Analyzer) CalculateNextBaseFee(baseFee *big.Int, gasUsed, gasLimit uint64) *big.Int {
	if baseFee == nil {
		return big.NewInt(1_000_000_000) // 기본 1 gwei
	}

	targetGas := gasLimit / 2
	if gasUsed == targetGas {
		return new(big.Int).Set(baseFee)
	}

	nextBaseFee := new(big.Int)

	//baseFee + (baseFee * (used - target) / target / 8)
	//baseFee - (baseFee * (target - used) / target / 8)
	denominator := new(big.Int).SetUint64(8)
	targetGasBI := new(big.Int).SetUint64(targetGas)

	if gasUsed > targetGas {
		gasGap := new(big.Int).SetUint64(gasUsed - targetGas)
		num := new(big.Int).Mul(baseFee, gasGap)

		diff := new(big.Int).Div(num, targetGasBI)
		diff.Div(diff, denominator)

		if diff.Sign() == 0 {
			diff.SetUint64(1)
		}
		nextBaseFee.Add(baseFee, diff)
	} else {
		gasGap := new(big.Int).SetUint64(targetGas - gasUsed)
		num := new(big.Int).Mul(baseFee, gasGap)

		diff := new(big.Int).Div(num, targetGasBI)
		diff.Div(diff, denominator)

		nextBaseFee.Sub(baseFee, diff)
	}

	if nextBaseFee.Sign() < 0 {
		nextBaseFee.SetUint64(0)
	}

	return nextBaseFee
}

func (a *Analyzer) CalculateWeightForGasUsed(gasUsed, gasLimit uint64) float64 {
	if gasLimit == 0 {
		return 0
	}

	//return math.Sqrt(float64(gasUsed) / float64(gasLimit))
	return math.Log(float64(gasUsed))
}
