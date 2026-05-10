package blockstore

import (
	"math/big"
	"sync"
)

type Ring struct {
	data  [][]*big.Int
	size  int
	index int
	full  bool
	mu    sync.RWMutex
}

func NewRing(size int) *Ring {
	if size <= 0 {
		size = 1 // 최소 1 보장 (panic 방지)
	}

	return &Ring{
		data:  make([][]*big.Int, size),
		size:  size,
		index: 0,
		full:  false,
	}
}

func (r *Ring) Add(tips []*big.Int) {
	if tips == nil {
		return // nil 방어
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.size == 0 {
		return
	}

	r.data[r.index] = tips
	r.index = (r.index + 1) % r.size

	if r.index == 0 {
		r.full = true
	}
}

func (r *Ring) All() [][]*big.Int {
	r.mu.RLock() //read
	defer r.mu.RUnlock()

	if r.size == 0 {
		return nil
	}

	if !r.full {
		out := make([][]*big.Int, r.index) // 복사해서 반환 (외부에서 수정 방지)
		copy(out, r.data[:r.index])
		return out
	}

	out := make([][]*big.Int, 0, r.size)
	out = append(out, r.data[r.index:]...) // idx~ end
	out = append(out, r.data[:r.index]...) // 0~idx-1
	return out
}
