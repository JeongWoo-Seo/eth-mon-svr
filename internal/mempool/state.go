package mempool

import (
	"strconv"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/ethereum/go-ethereum/core/types"
)

type TxState struct {
	Hash      string
	Nonce     uint64
	GasFeeCap string
	Timestamp time.Time
}

type State struct {
	data map[string]TxState
	mu   sync.Mutex
}

func NewState() *State {
	s := &State{
		data: make(map[string]TxState),
	}

	go s.cleaner()

	return s
}

func (s *State) Upset(tx *types.Transaction) {
	key := strconv.FormatUint(tx.Nonce(), 10)
	state := TxState{
		Hash:      tx.Hash().Hex(),
		Nonce:     tx.Nonce(),
		GasFeeCap: tx.GasFeeCap().String(),
		Timestamp: time.Now(),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	old, ok := s.data[key]
	if ok {
		if state.GasFeeCap > old.GasFeeCap {
			s.data[key] = state
			report.IncMempoolStored()
		}
	} else {
		s.data[key] = state
		report.IncMempoolStored()
	}
}

func (s *State) cleaner() {
	ticker := time.NewTicker(10 * time.Second)

	for range ticker.C {
		now := time.Now()

		s.mu.Lock()
		for h, v := range s.data {
			if now.Sub(v.Timestamp) > 2*time.Minute {
				delete(s.data, h)
			}
		}
		s.mu.Unlock()
	}
}
