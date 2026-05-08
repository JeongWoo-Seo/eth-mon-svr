package blockstore

import "sync"

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

func (s *Store) AddBlock(block BlockData) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.blocks = append([]BlockData{block}, s.blocks...)
	if len(s.blocks) > s.max {
		s.blocks = s.blocks[:s.max]
	}
}

func (s *Store) GetHistory() []BlockData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 안전한 접근을 위해 복사본 반환 권장 (생략 가능)
	return s.blocks
}
