package mempool

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

type entry struct {
	ExporeAt time.Time
}

type Cache struct {
	mu   sync.Mutex
	lru  *lru.Cache[string, entry]
	ttl  time.Duration
	size int
}

func NewCache(size int, ttl time.Duration) (*Cache, error) {
	c, err := lru.New[string, entry](size)
	if err != nil {
		return nil, err
	}

	return &Cache{
		lru:  c,
		ttl:  ttl,
		size: size,
	}, nil
}

func (c *Cache) Seen(txHash string) bool {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if v, ok := c.lru.Peek(txHash); ok {
		if now.Before(v.ExporeAt) {
			return true
		}
	}

	expireAt := now.Add(c.ttl)
	c.lru.Add(txHash, entry{
		ExporeAt: expireAt,
	})
	return false
}
