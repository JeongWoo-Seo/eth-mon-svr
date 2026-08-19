package blockstore

import (
	"sync"
)

type Store struct {
	mu     sync.RWMutex
	blocks []BlockData
	max    int
}

func NewBlockStore(maxSize int) *Store {
	return &Store{
		max:    maxSize,
		blocks: make([]BlockData, 0, maxSize),
	}
}

func (s *Store) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.blocks = make([]BlockData, 0, s.max)
}

func (s *Store) AddBlock(blockData BlockData) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.blocks = append([]BlockData{blockData}, s.blocks...)
	if len(s.blocks) > s.max {
		s.blocks = s.blocks[:s.max]
	}
}

func (s *Store) GetBlockData() []BlockData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]BlockData, len(s.blocks))
	copy(res, s.blocks)

	return res
}
