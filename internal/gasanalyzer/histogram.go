package gasanalyzer

import (
	"errors"
	"math"
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
	buckets := h.Snapshot() // 스냅샷으로 동기화된 버킷 슬라이스 획득

	if len(buckets) == 0 {
		return nil, errors.New("no data in histogram snapshots")
	}

	// 1. 전체 가중치(이미 Sqrt 처리된 가스의 총합) 계산
	var totalGas float64
	for _, b := range buckets {
		totalGas += float64(b.gasSum)
	}

	if totalGas == 0 {
		return nil, errors.New("no data in histogram")
	}

	// 2. 각 가스 레벨(Target)별 독립적인 백분위 연산
	for _, target := range targets {
		// 목표로 하는 누적 가스량 지점 계산
		targetValue := totalGas * target.Percentile

		// ✨ [핵심 교정] 각 타겟마다 인덱스와 누적 가스 합산량을 0에서부터 새로 시작합니다.
		bucketIdx := 0
		var curGasSum float64
		found := false

		for bucketIdx < len(buckets) {
			b := buckets[bucketIdx]
			weight := float64(b.gasSum)

			// 현재 버킷까지 더했을 때 목표치를 돌파하는지 확인
			if curGasSum+weight >= targetValue {
				if b.gasSum == 0 || b.maxTip <= b.minTip {
					// 가스가 없거나 단일 가격 버킷이면 최하값 선택
					result[target.Name] = b.minTip
				} else {
					// 버킷 내부에서 목표 지점이 어디쯤 위치하는지 비율(ratio) 계산
					r := targetValue - curGasSum
					ratio := r / weight

					// Floating-point 연산 오차 방지를 위한 상하한 클램핑
					if ratio < 0 {
						ratio = 0
					}
					if ratio > 1 {
						ratio = 1
					}

					// 버킷의 minTip과 maxTip 사이를 ratio 비율만큼 정교하게 보간(Interpolation)
					// 소수점 처리를 위해 math.Round 적용
					tip := b.minTip + uint64(math.Round(float64(b.maxTip-b.minTip)*ratio))

					// 계산된 결과가 해당 버킷의 최대 경계선을 넘지 않도록 안전장치
					if tip > b.maxTip {
						tip = b.maxTip
					}
					result[target.Name] = tip
				}
				found = true
				break
			}

			// 목표치에 도달하지 못했다면 가중치를 누적하고 다음 버킷으로 이동
			curGasSum += weight
			bucketIdx++
		}

		// 3. 만약 모든 버킷을 다 돌았는데도 targetValue를 못 채웠다면 (예: ratio가 1.0인 경우 등)
		// 가장 최상위에 위치한 버킷의 maxTip을 부여하여 안전하게 마감합니다.
		if !found {
			result[target.Name] = buckets[len(buckets)-1].maxTip
		}
	}

	return result, nil
}
