package main

import (
	"container/list"
	"sync"
)

// LRU is a primitive piece cache: hash → bytes, bounded by count. Count-
// not-memory bound is deliberate — torrents have fairly uniform piece
// sizes so a capacity in entries matches a rough memory cap, and that
// means no per-entry accounting on every hit.
type LRU struct {
	mu  sync.Mutex
	cap int
	ll  *list.List
	m   map[string]*list.Element
}

type lruEntry struct {
	key string
	val []byte
}

func newLRU(cap int) *LRU {
	return &LRU{
		cap: cap,
		ll:  list.New(),
		m:   make(map[string]*list.Element),
	}
}

func (c *LRU) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.m[key]

	if !ok {
		return nil, false
	}

	c.ll.MoveToFront(e)

	return e.Value.(*lruEntry).val, true
}

func (c *LRU) Put(key string, val []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.m[key]; ok {
		c.ll.MoveToFront(e)
		e.Value.(*lruEntry).val = val

		return
	}

	e := c.ll.PushFront(&lruEntry{key: key, val: val})
	c.m[key] = e

	if c.ll.Len() > c.cap {
		last := c.ll.Back()

		if last != nil {
			c.ll.Remove(last)
			delete(c.m, last.Value.(*lruEntry).key)
		}
	}
}
