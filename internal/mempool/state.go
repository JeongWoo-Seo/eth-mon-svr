package mempool

import (
	"strconv"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/ethereum/go-ethereum/core/types"
)

type State struct {
	data      map[string]PendingTxInfo
	hashToKey map[string]string
	mu        sync.Mutex
}

func NewState() *State {
	s := &State{
		data:      make(map[string]PendingTxInfo),
		hashToKey: make(map[string]string),
	}

	go s.cleaner()

	return s
}

func (s *State) UpsetBulk(txs []*types.Transaction) {
	if len(txs) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, tx := range txs {
		if tx == nil {
			continue
		}

		key := strconv.FormatUint(tx.Nonce(), 10)
		newFee := tx.GasFeeCap()

		old, exists := s.data[key]
		if exists {
			if newFee != nil && old.GasFeeCap != nil && newFee.Cmp(old.GasFeeCap) > 0 {
				s.update(key, tx)
				report.IncTxFeched(uint64(1))
			}
		} else {
			s.update(key, tx)
			report.IncTxFeched(uint64(1))
		}
	}
}

func (s *State) update(key string, tx *types.Transaction) {
	s.data[key] = PendingTxInfo{
		Hash:      tx.Hash().Hex(), //hex
		GasFeeCap: tx.GasFeeCap(),  //max fee
		GasTipCap: tx.GasTipCap(),
		Timestamp: time.Now(),
	}
	s.hashToKey[tx.Hash().Hex()] = key
}

func (s *State) Delete(txHash string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	nonceKey, exists := s.hashToKey[txHash]
	if !exists {
		return false
	}

	delete(s.data, nonceKey)
	delete(s.hashToKey, txHash)
	return true
}

func (s *State) DeleteBulk(hashes []string) int {
	if len(hashes) == 0 {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for _, h := range hashes {
		nonce, exist := s.hashToKey[h]
		if !exist {
			continue
		}

		delete(s.data, nonce)
		delete(s.hashToKey, h)
		removed++
	}
	return removed
}

func (s *State) Snapshot() map[string]PendingTxInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := make(map[string]PendingTxInfo, len(s.data))
	for k, v := range s.data {
		cp[k] = v
	}

	return cp
}

func (s *State) cleaner() {
	ticker := time.NewTicker(10 * time.Second)

	for range ticker.C {
		now := time.Now()

		s.mu.Lock()
		for h, v := range s.data {
			if now.Sub(v.Timestamp) > 2*time.Minute {
				delete(s.hashToKey, v.Hash)
				delete(s.data, h)
			}
		}
		s.mu.Unlock()
	}
}
