package mempool

import (
	"math/big"
	"strconv"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/ethereum/go-ethereum/core/types"
)

type TxState struct {
	Hash      string
	Nonce     uint64
	GasFeeCap *big.Int
	Timestamp time.Time
}

type State struct {
	data      map[string]TxState
	hashToKey map[string]string
	mu        sync.Mutex
}

func NewState() *State {
	s := &State{
		data:      make(map[string]TxState),
		hashToKey: make(map[string]string),
	}

	go s.cleaner()

	return s
}

func (s *State) Upset(tx *types.Transaction) {
	key := strconv.FormatUint(tx.Nonce(), 10)
	txHash := tx.Hash().Hex()
	newFee := tx.GasFeeCap()

	s.mu.Lock()
	defer s.mu.Unlock()

	old, exists := s.data[key]
	if exists {
		if newFee.Cmp(old.GasFeeCap) > 0 {
			delete(s.data, key)
			s.update(key, txHash, tx.Nonce(), newFee)
		}
	} else {
		s.update(key, txHash, tx.Nonce(), newFee)
		report.IncMempoolStored()
	}
}

func (s *State) update(key string, txHash string, nonce uint64, fee *big.Int) {
	s.data[key] = TxState{
		Hash:      txHash, //hex
		Nonce:     nonce,
		GasFeeCap: fee,
		Timestamp: time.Now(),
	}
	s.hashToKey[txHash] = key
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
