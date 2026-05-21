package mempool

import (
	"container/heap"
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
func (pool *PendingMemPool) CollectAndClean(minedHashes map[string]struct{}) []PendingTxInfo {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.heap.Len() == 0 {
		return nil
	}

	expireTime := time.Now().Add(-pool.ttl)

	oldHeap := *pool.heap
	survivedTxs := make([]PendingTxInfo, 0, len(oldHeap))

	for _, tx := range oldHeap {
		_, isMined := minedHashes[tx.Hash]

		if !isMined && tx.Timestamp.After(expireTime) {
			survivedTxs = append(survivedTxs, tx)
		}
	}

	*pool.heap = survivedTxs

	heap.Init(pool.heap)

	return survivedTxs
}

func (pool *PendingMemPool) Len() int {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return pool.heap.Len()
}
