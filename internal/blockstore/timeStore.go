package blockstore

import (
	"sync"
)

// 빠른 조회를 위해 map 형태로 저장하고, 삭제할 때는 큐 값을 활용해 빠르게 삭제
type BlockTimeStore struct {
	mu      sync.Mutex
	maxSize int
	Times   map[uint64]uint64
	queue   []uint64
}

func NewBlockTimeStore(maxSize uint64) *BlockTimeStore {
	return &BlockTimeStore{
		maxSize: int(maxSize),
		Times:   make(map[uint64]uint64),
		queue:   make([]uint64, 0, maxSize),
	}
}

func (b *BlockTimeStore) AddBlock(blockNum uint64, timestamp uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exist := b.Times[blockNum]; exist {
		b.Times[blockNum] = timestamp
		return
	}

	b.Times[blockNum] = timestamp
	b.queue = append(b.queue, blockNum)

	if len(b.queue) > b.maxSize {
		old := b.queue[0]
		delete(b.Times, old)
		b.queue = b.queue[1:]
	}
}

func (b *BlockTimeStore) GetTime(blockNum uint64) (uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	timestamp, ok := b.Times[blockNum]

	return timestamp, ok
}
