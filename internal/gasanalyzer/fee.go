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

func (a *Analyzer) CalculateNextBaseFee(baseFee *big.Int, gasUsed, gasLimit uint64) *big.Int {
	targetGas := gasLimit / 2

	//gasused와 targetGas가 동일하면 baseFee는 유지됨
	if gasUsed == targetGas {
		return new(big.Int).Set(baseFee)
	}

	nextBaseFee := new(big.Int)
	ta := new(big.Int).SetUint64(targetGas)
	ta.Mul(ta, big.NewInt(8))

	// basefee + (basefee * (gasused - targetgas) / targetgas / 8)
	// basefee - (basefee * (targetgas - gasused) / targetgas / 8)
	if gasUsed > targetGas {
		gasGap := new(big.Int).SetUint64(gasUsed - targetGas)
		mul := new(big.Int).Mul(baseFee, gasGap)
		di := new(big.Int).Div(mul, ta)
		//변화량이 작아 di 가 0인 경우 1wei로 설정함
		if di.Cmp(big.NewInt(0)) == 0 {
			di.SetInt64(1)
		}
		nextBaseFee.Add(baseFee, di)
	} else {
		gasGap := new(big.Int).SetUint64(targetGas - gasUsed)
		mul := new(big.Int).Mul(baseFee, gasGap)
		di := new(big.Int).Div(mul, ta)
		nextBaseFee.Sub(baseFee, di)

		//BaseFee가 음수인 경우
		if nextBaseFee.Cmp(big.NewInt(0)) < 0 {
			nextBaseFee.SetInt64(0)
		}
	}

	return nextBaseFee
}

func (a *Analyzer) CalculateWeightForGasUsed(gasUsed, gasLimit uint64) float64 {
	if gasLimit == 0 {
		return 0
	}

	return math.Sqrt(float64(gasUsed) / float64(gasLimit))
}
