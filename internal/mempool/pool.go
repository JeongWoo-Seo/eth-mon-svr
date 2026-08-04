package mempool

import (
	"fmt"
	"math/big"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type PendingMemPool struct {
	mu            sync.RWMutex
	Signer        types.Signer
	Accounts      map[common.Address]*AccountPending //from -> []nonce -> tx
	ExpireBuckets map[uint64][]*PendingTx            // blocknum -> []tx
	HashIndex     map[common.Hash]*PendingTx         //hash -> tx
	TTLBlock      uint64
}

func NewPendingMemPool(chainID string, ttlblock uint64) (*PendingMemPool, error) {
	id, ok := new(big.Int).SetString(chainID, 10)
	if !ok {
		return nil, fmt.Errorf("invalid chain ID: %q", chainID)
	}

	return &PendingMemPool{
		Signer:        types.LatestSignerForChainID(id),
		Accounts:      make(map[common.Address]*AccountPending),
		ExpireBuckets: make(map[uint64][]*PendingTx),
		HashIndex:     make(map[common.Hash]*PendingTx),
		TTLBlock:      ttlblock,
	}, nil
}

func (pool *PendingMemPool) PushBatch(txs []*types.Transaction, currentBlockNum, curBlockTime uint64) {
	if len(txs) == 0 {
		return
	}

	validTx := make([]*PendingTx, 0, len(txs))
	//락을 잡기 전에 데이터 변환 및 유효성 검사
	for _, tx := range txs {
		if tx == nil {
			continue
		}

		// GasTipCap이 없는 경우 예전 버전으로 GasPrice을 사용
		tipCap := tx.GasTipCap()
		if tipCap == nil {
			tipCap = tx.GasPrice()
		}

		feeCap := tx.GasFeeCap()
		if feeCap == nil {
			feeCap = tx.GasPrice()
		}

		if tipCap == nil || feeCap == nil || tx.Gas() == 0 {
			continue
		}

		//오버플로우 대비
		if !feeCap.IsUint64() || !tipCap.IsUint64() {
			continue
		}

		from, err := types.Sender(pool.Signer, tx)
		if err != nil {
			continue
		}

		validTx = append(validTx, &PendingTx{
			Hash:          tx.Hash(),
			From:          from,
			Nonce:         tx.Nonce(),
			FeeCap:        feeCap.Uint64(),
			TipCap:        tipCap.Uint64(),
			GasLimit:      tx.Gas(),
			SeenBlock:     currentBlockNum,
			SeenBlockTime: curBlockTime,
			ExpireBlock:   currentBlockNum + pool.TTLBlock,
			ExpireIndex:   -1,
		})
	}

	if len(validTx) == 0 {
		return
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	for _, tx := range validTx {
		// 새로운 계좌를 accouts에 추가
		account, ok := pool.Accounts[tx.From]
		if !ok {
			account = &AccountPending{
				NonceMap: make(map[uint64]*PendingTx),
			}
		}

		//기존 tx를 replacemenet/cancel 한다면 기존 hash 값을 삭제
		if old, ok := account.NonceMap[tx.Nonce]; ok {
			if tx.TipCap <= old.TipCap || tx.FeeCap <= old.FeeCap {
				continue
			}

			pool.removeExpireBucket(old)
			delete(pool.HashIndex, old.Hash)
		}

		account.NonceMap[tx.Nonce] = tx
		pool.Accounts[tx.From] = account
		pool.HashIndex[tx.Hash] = tx
		pool.addExpireBucket(tx)
	}
}

func (pool *PendingMemPool) addExpireBucket(tx *PendingTx) {
	list := pool.ExpireBuckets[tx.ExpireBlock]
	tx.ExpireIndex = len(list)
	pool.ExpireBuckets[tx.ExpireBlock] = append(list, tx)
}

func (pool *PendingMemPool) removeExpireBucket(tx *PendingTx) {
	list, ok := pool.ExpireBuckets[tx.ExpireBlock]
	if !ok || len(list) == 0 {
		return
	}

	index := tx.ExpireIndex
	if index < 0 || index >= len(list) {
		return
	}

	if list[index].Hash != tx.Hash {
		return
	}

	last := len(list) - 1

	if index != last {
		list[index] = list[last]
		list[index].ExpireIndex = index
	}

	list = list[:last]

	if len(list) == 0 {
		delete(pool.ExpireBuckets, tx.ExpireBlock)
	} else {
		pool.ExpireBuckets[tx.ExpireBlock] = list
	}
}

// block에 포함된 tx는 mempool에서 제외
func (pool *PendingMemPool) RemoveByReceipts(receipts []*types.Receipt) int {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	removed_cnt := 0
	for _, receipt := range receipts {
		if pool.removeByHash(receipt.TxHash) {
			removed_cnt++
		}
	}

	return removed_cnt
}

func (pool *PendingMemPool) removeByHash(hash common.Hash) bool {
	tx, ok := pool.HashIndex[hash]
	if !ok {
		return false
	}

	account, ok := pool.Accounts[tx.From]
	if ok {
		delete(account.NonceMap, tx.Nonce)
		if len(account.NonceMap) == 0 {
			delete(pool.Accounts, tx.From)
		}
	}

	delete(pool.HashIndex, hash)

	pool.removeExpireBucket(tx)

	return true
}

func (pool *PendingMemPool) RemoveExpired(curBlock uint64) int {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	removed_cnt := 0

	for block, list := range pool.ExpireBuckets {
		if block > curBlock {
			continue
		}
		for _, tx := range list {
			account := pool.Accounts[tx.From]
			if account == nil {
				continue
			}

			storedTx, ok := account.NonceMap[tx.Nonce]
			if !ok || storedTx.Hash != tx.Hash {
				continue
			}
			delete(account.NonceMap, tx.Nonce)

			if len(account.NonceMap) == 0 {
				delete(pool.Accounts, tx.From)
			}

			delete(pool.HashIndex, tx.Hash)
			removed_cnt++
		}

		delete(pool.ExpireBuckets, block)
	}

	return removed_cnt
}

// pending tx 데이터를 넘길때 nonce gap 여부를 확인후 넘김
func (pool *PendingMemPool) Snapshot() []PendingTx {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	data := make([]PendingTx, 0, len(pool.HashIndex))

	for _, account := range pool.Accounts {
		nonces := make([]uint64, 0, len(account.NonceMap))
		for nonce := range account.NonceMap {
			nonces = append(nonces, nonce)
		}

		if len(nonces) == 0 {
			continue
		}

		sort.Slice(nonces, func(i, j int) bool {
			return nonces[i] < nonces[j]
		})

		expected := nonces[0]
		hasGap := false

		for _, nonce := range nonces {
			if nonce != expected {
				hasGap = true
			}

			tx := *account.NonceMap[nonce]
			tx.NonceGap = hasGap

			data = append(data, tx)

			expected = nonce + 1
		}
	}

	return data
}
