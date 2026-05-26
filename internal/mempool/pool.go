package mempool

import (
	"container/heap"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

type PendingMemPool struct {
	mu      sync.RWMutex
	heap    *minTipHeap
	maxSize int
	ttl     time.Duration
}

func NewPendingMemPool(maxSize int, ttl time.Duration) *PendingMemPool {
	h := &minTipHeap{}
	heap.Init(h)

	return &PendingMemPool{
		heap:    h,
		maxSize: maxSize,
		ttl:     ttl,
	}
}

func (pool *PendingMemPool) PushBatch(txs []*types.Transaction) {
	if len(txs) == 0 {
		return
	}

	now := time.Now()

	//락을 잡기 전에 데이터 변환 및 유효성 검사
	validTxs := make([]PendingTxInfo, 0, len(txs))
	for _, tx := range txs {
		if tx == nil {
			continue
		}

		// GasTipCap이 없는 경우 예전 버전으로 GasPrice을 사용
		tipCap := tx.GasTipCap()
		if tipCap == nil {
			tipCap = tx.GasPrice()
		}

		if tipCap == nil || tx.Gas() == 0 {
			continue
		}

		validTxs = append(validTxs, PendingTxInfo{
			Hash:      tx.Hash().Hex(),
			GasFeeCap: tx.GasFeeCap(),
			GasTipCap: tipCap,
			Gas:       tx.Gas(),
			Timestamp: now,
		})
	}

	if len(validTxs) == 0 {
		return
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	for _, tx := range validTxs {
		if pool.heap.Len() < pool.maxSize {
			heap.Push(pool.heap, tx)
		} else {
			lst := pool.heap.Peak()
			if lst.GasTipCap != nil && tx.GasTipCap.Cmp(lst.GasTipCap) > 0 {
				heap.Pop(pool.heap)
				heap.Push(pool.heap, tx)
			}
		}
	}
}

// 오래된 tx이거나 block에 포함된 tx는 mempool에서 제외
func (pool *PendingMemPool) CollectAndClean(minedHashes map[string]struct{}) int {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	initialCount := pool.heap.Len()
	if initialCount == 0 {
		return 0
	}

	expireTime := time.Now().Add(-pool.ttl)
	oldHeap := *pool.heap

	//-------------------------------------------------------------------------
	// 1단계: 마이닝되지 않고 만료되지 않은 '진짜 남은' 트랜잭션만 먼저 1차 선별
	//-------------------------------------------------------------------------
	candidates := make([]PendingTxInfo, 0, len(oldHeap))
	for _, tx := range oldHeap {
		_, isMined := minedHashes[tx.Hash]

		// 블록에 포함되지 않았고, TTL 만료도 되지 않은 트랜잭션만 후보군에 추가
		if !isMined && tx.Timestamp.After(expireTime) {
			candidates = append(candidates, tx)
		}
	}

	//-------------------------------------------------------------------------
	// 2단계: 남은 트랜잭션 중에서 상위 고가 팁 3개 추적 및 제거
	//-------------------------------------------------------------------------
	topTxsToRemove := 3
	if topTxsToRemove > len(candidates) {
		topTxsToRemove = len(candidates)
	}

	// 후보군을 팁(GasTipCap) 기준 내림차순 정렬 (비싼 팁이 index 0으로)
	sort.Slice(candidates, func(i, j int) bool {
		// GasTipCap이 nil일 경우를 대비한 방어 코드 (필요 시 유지)
		if candidates[i].GasTipCap == nil {
			return false
		}
		if candidates[j].GasTipCap == nil {
			return true
		}

		// c1.Cmp(c2)는 c1 > c2 이면 1, 같으면 0, 작으면 -1을 반환합니다.
		// 내림차순 정렬(큰 값이 앞으로)이므로 1인 경우 true를 반환하도록 합니다.
		return candidates[i].GasTipCap.Cmp(candidates[j].GasTipCap) == 1
	})

	// 상위 3개를 제외한 나머지 진짜 생존자들만 담을 슬라이스
	// candidates[topTxsToRemove:]를 바로 써도 되지만, 안전하게 새로 할당하거나 슬라이싱합니다.
	survivedTxs := candidates[topTxsToRemove:]

	//-------------------------------------------------------------------------
	// 3단계: MemPool 힙 갱신 및 Min-Heap 구조 재정렬
	//-------------------------------------------------------------------------
	*pool.heap = survivedTxs

	// 슬라이스의 요소와 순서가 완전히 바뀌었으므로
	// 루트 노드에 다시 최솟값이 오도록 Min-Heap 구조 재조정 (Heapify)
	heap.Init(pool.heap)

	// 최종적으로 제거된 총 트랜잭션 개수 반환 (마이닝 + 만료 + 상위 노이즈 제거)
	return initialCount - len(survivedTxs)
}

func (pool *PendingMemPool) Len() int {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return pool.heap.Len()
}

func (pool *PendingMemPool) Snapshot() []PendingTxInfo {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	size := pool.heap.Len()
	if size == 0 {
		return nil
	}

	data := make([]PendingTxInfo, size)
	copy(data, *pool.heap)

	// big.Int 내부 값 포인터가 오염되지 않도록 Deep Copy
	for i := 0; i < size; i++ {
		if data[i].GasTipCap != nil {
			data[i].GasTipCap = new(big.Int).Set(data[i].GasTipCap)
		}
		if data[i].GasFeeCap != nil {
			data[i].GasFeeCap = new(big.Int).Set(data[i].GasFeeCap)
		}
	}

	return data
}
