package ingestion

import (
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

type Cache struct {
	mu  sync.Mutex
	lru *lru.Cache[string, struct{}]
}

func NewCache(size int) (*Cache, error) {
	c, err := lru.New[string, struct{}](size)
	if err != nil {
		return nil, err
	}

	return &Cache{
		lru: c,
	}, nil
}

func (c *Cache) Seen(txHash string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.lru.Get(txHash); ok {
		return true
	}

	c.lru.Add(txHash, struct{}{})
	return false
}
