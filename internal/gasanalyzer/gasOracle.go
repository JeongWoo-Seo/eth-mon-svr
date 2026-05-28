package gasanalyzer

import (
	"fmt"
	"math"
)

type GasOracle struct {
	BlockHist   *Histogram
	PendingHist *Histogram
}

func NewGasOracle() *GasOracle {
	return &GasOracle{
		BlockHist:   NewHistogram(),
		PendingHist: NewHistogram(),
	}
}

func (o *GasOracle) GasPrediction() (map[string]uint64, error) {
	blockPrice, blockErr := o.BlockHist.PercentileGas(gasPredictionTargets)
	pendingPrice, pendingErr := o.PendingHist.PercentileGas(gasPredictionTargets)

	if blockErr == nil || pendingErr == nil {
		return nil, fmt.Errorf("both histograms have no data")
	}

	result := make(map[string]uint64)

	for _, target := range gasPredictionTargets {
		name := target.Name
		bp, hasBlock := blockPrice[name]
		pp, hasPending := pendingPrice[name]

		// 한 쪽에만 데이터가 있는 경우
		if !hasBlock {
			result[name] = pp
			continue
		}
		if !hasPending {
			result[name] = bp
			continue
		}

		if target.Ratio <= 0.30 {
			// Ratio가 30% 이하인 등급 (예: low)
			result[name] = bp
		} else {
			// Ratio가 30%를 초과하는 등급 (예: market, fast, urgent)
			result[name] = uint64(math.Max(float64(bp), float64(pp)))
		}
	}
	return result, nil
}
