package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// Prefetcher sits in front of Storage.Cat for SFTP reads. Three jobs:
//
//   - Dedupe concurrent fetches of the same piece via singleflight.
//   - Readahead into the LRU so sequential reads find pieces warm.
//   - Bound memory/concurrency: a fixed pool of worker goroutines
//     drains a bounded queue. Prefetch() enqueues hashes non-blocking;
//     if the queue is full we drop — the foreground reader will
//     fetch synchronously when it gets there. Earlier designs
//     spawned a goroutine per prefetch request, which blew up
//     spectacularly at K=5000 (millions of goroutines queued on a
//     smaller sem, OOM-killed).
type Prefetcher struct {
	store *Storage
	cache *LRU
	cfg   *Config

	sf    singleflight.Group
	queue chan string
	sem   chan struct{}

	hits     atomic.Int64
	misses   atomic.Int64
	preIss   atomic.Int64 // handed to the queue
	preDrop  atomic.Int64 // dropped because queue was full
	preHit   atomic.Int64 // already-in-cache at enqueue time
	fetchN   atomic.Int64 // completed GetObject calls (unique, via singleflight)
	fetchNs  atomic.Int64 // cumulative GetObject latency in ns
	inFlight atomic.Int64 // concurrent GetObject calls in progress
}

func newPrefetcher(cfg *Config, store *Storage, cache *LRU, concurrency int) *Prefetcher {
	// Queue headroom: the workers drain as fast as GetObject will
	// let them, so the queue mostly absorbs the burst created by a
	// single getPiece() call (K items). A few multiples of
	// concurrency is plenty — more just buffers stale prefetch
	// targets that will be evicted by the time their turn comes.
	queueSize := concurrency * 4

	p := &Prefetcher{
		store: store,
		cache: cache,
		cfg:   cfg,
		queue: make(chan string, queueSize),
		sem:   make(chan struct{}, concurrency),
	}

	for i := 0; i < concurrency; i++ {
		go p.worker()
	}

	return p
}

func (p *Prefetcher) worker() {
	for h := range p.queue {
		if _, ok := p.cache.Get(h); ok {
			continue
		}

		exc := Try(func() {
			p.Get(h)
		})

		exc.Catch(func(e *Exception) {
			fmt.Fprintln(os.Stderr, clr(clrY, "prefetch: "+h+": "+e.Error()))
		})
	}
}

// Get returns piece bytes, caching the result. Concurrent Gets for
// the same piece collapse into one underlying GetObject via
// singleflight.
func (p *Prefetcher) Get(hash string) []byte {
	if data, ok := p.cache.Get(hash); ok {
		p.hits.Add(1)

		return data
	}

	v, _, _ := p.sf.Do(hash, func() (any, error) {
		if data, ok := p.cache.Get(hash); ok {
			return data, nil
		}

		p.sem <- struct{}{}
		defer func() { <-p.sem }()

		p.inFlight.Add(1)
		t0 := time.Now()

		data := p.store.Cat(p.cfg.KeyPiece(hash))

		p.fetchNs.Add(time.Since(t0).Nanoseconds())
		p.fetchN.Add(1)
		p.inFlight.Add(-1)

		p.cache.Put(hash, data)

		return data, nil
	})

	p.misses.Add(1)

	return v.([]byte)
}

// Prefetch enqueues hashes for asynchronous fetching. Non-blocking —
// if the queue is full we stop iterating (the remaining hashes, all
// farther from the reader, would just get dropped anyway). Pieces
// already in cache are skipped at enqueue time.
//
// The early exit matters a lot at large K. On each getPiece we get
// called with K hashes; iterating to 5000 takes 5000 cache.Get calls
// against the LRU mutex, and with workers + foreground all
// contending, that alone becomes the bottleneck long before the
// extra prefetches could ever land usefully.
func (p *Prefetcher) Prefetch(hashes []string) {
	for i, h := range hashes {
		if _, ok := p.cache.Get(h); ok {
			p.preHit.Add(1)

			continue
		}

		select {
		case p.queue <- h:
			p.preIss.Add(1)

		default:
			// Account for everything we didn't even try — gives
			// a truthful ratio of "asked vs delivered to
			// workers" when K is oversized.
			p.preDrop.Add(int64(len(hashes) - i))

			return
		}
	}
}

func (p *Prefetcher) Stats() string {
	n := p.fetchN.Load()
	avgMs := 0.0

	if n > 0 {
		avgMs = float64(p.fetchNs.Load()) / float64(n) / 1e6
	}

	return fmt.Sprintf(
		"hits=%d misses=%d pf-issued=%d pf-dropped=%d pf-already-hit=%d fetched=%d inflight=%d/%d queue=%d/%d avg-fetch=%.1fms",
		p.hits.Load(), p.misses.Load(),
		p.preIss.Load(), p.preDrop.Load(), p.preHit.Load(),
		n, p.inFlight.Load(), cap(p.sem),
		len(p.queue), cap(p.queue), avgMs)
}
