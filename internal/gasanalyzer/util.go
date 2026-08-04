package gasanalyzer

import "sort"

func totalWeight(poolData []WeightedTip) float64 {
	var total float64
	for _, tx := range poolData {
		total += tx.Weight
	}

	return total
}

func buildPrediction(percentiles []uint64) map[string]uint64 {
	result := make(map[string]uint64, len(GasAnalysisTargets))

	for _, target := range GasPredictionTargets {
		group := PredictionGroups[target.GroupKey]

		if len(group) == 0 {
			continue
		}

		result[target.Name] = calculateWeightedValue(percentiles, group)
	}

	return result
}

func calculateWeightedPercentiles(poolData []WeightedTip) []uint64 {
	totalWeight := totalWeight(poolData)

	if totalWeight == 0 {
		return nil
	}

	results := make([]uint64, len(GasAnalysisTargets))

	var cumulative float64
	index := 0

	for _, tx := range poolData {
		cumulative += tx.Weight
		for index < len(GasAnalysisTargets) && cumulative >= GasAnalysisTargets[index]*totalWeight {
			results[index] = tx.Tip
			index++
		}

		if index >= len(results) {
			break
		}
	}

	// 부족한 percentile은 마지막 값 사용
	lastTip := poolData[len(poolData)-1].Tip
	for index < len(results) {
		results[index] = lastTip
		index++
	}

	return results
}

func calculateWeightedValue(values []uint64, group []WeightPoint) uint64 {
	var sum float64
	var weight float64

	for _, wp := range group {
		//0 <= index <= len(values) 범위를 넘는 것을 방지하기 위해
		if wp.Index < 0 || wp.Index >= len(values) {
			continue
		}

		sum += float64(values[wp.Index]) * wp.Weight
		weight += wp.Weight
	}

	if weight == 0 {
		return 0
	}

	return uint64(sum / weight)
}

func calculateOuterBound(poolData []WeightedTip) OuterInfo {
	if len(poolData) == 0 {
		return OuterInfo{
			Count:      0,
			UpperBound: 0,
		}
	}

	totalSum := totalWeight(poolData)
	if totalSum == 0 {
		return OuterInfo{
			Count:      0,
			UpperBound: 0,
		}
	}

	//p95계산
	var p95 uint64
	var sumWeight float64
	for _, tx := range poolData {
		sumWeight += tx.Weight

		if sumWeight >= totalSum*0.95 {
			p95 = tx.Tip
			break
		}
	}

	if p95 == 0 {
		return OuterInfo{
			Count:      0,
			UpperBound: 0,
		}
	}

	upperBound := p95 * 2
	if upperBound < p95 { //오버플로 발생시
		upperBound = ^uint64(0) //uint64 최대값
	}

	idx := sort.Search(len(poolData), func(i int) bool {
		return poolData[i].Tip > upperBound
	})

	return OuterInfo{
		Count:      len(poolData) - idx,
		UpperBound: upperBound,
	}
}

func defaultValue() (map[string]uint64, uint64) {
	return map[string]uint64{
		"low":    500_000_000,   // Base + 1 Gwei
		"market": 750_000_000,   // Base + 1.5 Gwei
		"fast":   1_000_000_000, // Base + 2 Gwei
		"urgent": 1_500_000_000, // Base + 5 Gwei
	}, 1_000_000
}
