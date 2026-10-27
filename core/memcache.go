package core

import (
	"container/list"
	"sync"
)

// memCache is a byte-budgeted LRU of verified pack members, so that reading a
// small file in several ranges costs one pack read.
type memCache struct {
	mu     sync.Mutex
	budget int64
	used   int64
	ll     *list.List
	m      map[string]*list.Element
}

type memItem struct {
	key  string
	data []byte
}

func newMemCache(budget int64) *memCache {
	return &memCache{budget: budget, ll: list.New(), m: map[string]*list.Element{}}
}

func (c *memCache) get(key string) ([]byte, bool) {
	if c.budget <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.m[key]; ok {
		c.ll.MoveToFront(e)
		return e.Value.(*memItem).data, true
	}
	return nil, false
}

func (c *memCache) put(key string, data []byte) {
	if c.budget <= 0 || int64(len(data)) > c.budget/4 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.m[key]; ok {
		return
	}
	c.m[key] = c.ll.PushFront(&memItem{key, data})
	c.used += int64(len(data))
	for c.used > c.budget {
		e := c.ll.Back()
		it := e.Value.(*memItem)
		c.ll.Remove(e)
		delete(c.m, it.key)
		c.used -= int64(len(it.data))
	}
}
