package gasanalyzer

import (
	"errors"
	"sync"
)

type bucket struct {
	minTip uint64
	maxTip uint64

	count  int
	gasSum uint64
}

type Histogram struct {
	Buckets []bucket
	mu      sync.RWMutex
}

func NewHistogram() *Histogram {
	ranges := []uint64{
		1_000_000,
		2_000_000,
		5_000_000,
		10_000_000,
		20_000_000,
		50_000_000,
		100_000_000,
		200_000_000,
		500_000_000,
		1_000_000_000,
		2_000_000_000,
		5_000_000_000,
		10_000_000_000,
		20_000_000_000,
	}

	buckets := make([]bucket, 0)

	for i := 0; i < len(ranges)-1; i++ {
		buckets = append(buckets, bucket{
			minTip: ranges[i],
			maxTip: ranges[i+1],
		})
	}

	return &Histogram{
		Buckets: buckets,
	}
}

func (h *Histogram) Add(txs []GasTip) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, tx := range txs {
		i := h.bucketIndex(tx.Tip)

		h.Buckets[i].count++
		h.Buckets[i].gasSum += tx.Gas
	}
}

func (h *Histogram) bucketIndex(tip uint64) int {
	for i := range h.Buckets {
		if tip >= h.Buckets[i].minTip && tip < h.Buckets[i].maxTip {
			return i
		}
	}

	return len(h.Buckets) - 1
}

func (h *Histogram) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := range h.Buckets {
		h.Buckets[i].count = 0
		h.Buckets[i].gasSum = 0
	}
}

func (h *Histogram) Snapshot() []bucket {
	h.mu.RLock()
	defer h.mu.RUnlock()

	cp := make([]bucket, len(h.Buckets))
	copy(cp, h.Buckets)

	return cp
}

func (h *Histogram) PercentileGas(targets []TargetPercentile) (map[string]uint64, error) {
	if len(targets) == 0 {
		return nil, errors.New("targets elements cannot be empty")
	}

	result := make(map[string]uint64)
	buckets := h.Snapshot()
	var totalGas float64

	for _, b := range h.Buckets {
		totalGas += float64(b.gasSum)
	}

	if totalGas == 0 {
		return nil, errors.New("no data in histogram")
	}

	bucketIdx := 0
	var curGasSum float64

	for _, target := range targets {
		targetValue := totalGas * target.Ratio

		for bucketIdx < len(buckets) {
			b := buckets[bucketIdx]
			weight := float64(b.gasSum)
			if curGasSum+weight >= targetValue {
				if b.gasSum == 0 { // gassum 이 0면 최하값으로
					result[target.Name] = b.minTip
				} else { // 아니면 비율로 적용
					r := targetValue - curGasSum
					ratio := r / weight
					tip := b.minTip + uint64(float64(b.maxTip-b.minTip)*ratio)
					result[target.Name] = tip
				}
				break
			}

			curGasSum += weight
			bucketIdx++
		}

		// bucket 내부를 다 확인 후 아직 결과를 다 채우지 못했을 때 최상위 결과값으로 채움
		if bucketIdx >= len(buckets) {
			result[target.Name] = buckets[len(buckets)-1].maxTip
		}
	}

	return result, nil
}
