package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// Prefetcher runs background GetObjects into the LRU, following
// ranges the reader submits. Design points:
//
//   - Submit() is blocking. Natural backpressure: if workers can't
//     keep up, the reader stalls at submit instead of racing ahead
//     and filling memory with pending work.
//   - Queue items are ranges (slices of piece hashes), not individual
//     hashes. One channel send per reader step regardless of K —
//     matters when --prefetch-k is in the thousands.
//   - One worker processes one range at a time, sequentially through
//     its pieces. Concurrency comes from N workers (= --up-parallel)
//     pulling different ranges in parallel.
//   - Each worker maintains a local set of hashes it has already
//     fetched in the current epoch (= last 100 ranges). Prevents
//     re-entering the LRU mutex + singleflight machinery when
//     consecutive reader windows overlap. The set is reset
//     periodically so memory stays bounded and stale entries don't
//     accumulate.
//   - Singleflight dedupes across workers: two workers reaching the
//     same hash (different ranges, not caught by local sets) collapse
//     onto one GetObject.
type Prefetcher struct {
	store *Storage
	cache *LRU
	cfg   *Config

	sf     singleflight.Group
	ranges chan []string

	workers int

	hits         atomic.Int64
	misses       atomic.Int64
	submitted    atomic.Int64 // ranges successfully enqueued
	skippedLocal atomic.Int64 // hashes skipped by worker-local epoch set
	fetchN       atomic.Int64 // unique GetObject calls (via singleflight)
	fetchNs      atomic.Int64 // cumulative GetObject latency
	inFlight     atomic.Int64 // concurrent GetObject calls
}

// workerEpoch is how many ranges a worker processes before wiping its
// local "already-done" set. 100 is a round number; big enough that
// typical overlapping windows stay deduplicated, small enough that
// the set doesn't grow without bound.
const workerEpoch = 100

func newPrefetcher(cfg *Config, store *Storage, cache *LRU, workers int) *Prefetcher {
	p := &Prefetcher{
		store:   store,
		cache:   cache,
		cfg:     cfg,
		ranges:  make(chan []string, workers*2),
		workers: workers,
	}

	for i := 0; i < workers; i++ {
		go p.worker()
	}

	return p
}

// Submit enqueues a range of piece hashes for asynchronous fetching.
// Blocks if the queue is full — that backpressure keeps readers from
// outrunning the fetcher.
func (p *Prefetcher) Submit(hashes []string) {
	if len(hashes) == 0 {
		return
	}

	p.ranges <- hashes
	p.submitted.Add(1)
}

func (p *Prefetcher) worker() {
	seen := make(map[string]struct{})
	processed := 0

	for r := range p.ranges {
		for _, h := range r {
			if _, ok := seen[h]; ok {
				p.skippedLocal.Add(1)

				continue
			}

			seen[h] = struct{}{}

			exc := Try(func() {
				p.fetch(h)
			})

			exc.Catch(func(e *Exception) {
				fmt.Fprintln(os.Stderr, clr(clrY, "prefetch: "+h+": "+e.Error()))
			})
		}

		processed++

		if processed >= workerEpoch {
			seen = make(map[string]struct{})
			processed = 0
		}
	}
}

// Get is the foreground read path — immediate, no ranges.
func (p *Prefetcher) Get(hash string) []byte {
	if data, ok := p.cache.Get(hash); ok {
		p.hits.Add(1)

		return data
	}

	p.misses.Add(1)

	return p.fetch(hash)
}

// fetch runs through singleflight so concurrent requests for the
// same hash — foreground racing a prefetch, or two overlapping
// worker ranges — share one underlying GetObject.
func (p *Prefetcher) fetch(hash string) []byte {
	v, _, _ := p.sf.Do(hash, func() (any, error) {
		if data, ok := p.cache.Get(hash); ok {
			return data, nil
		}

		p.inFlight.Add(1)
		t0 := time.Now()

		data := p.store.Cat(p.cfg.KeyPiece(hash))

		p.fetchNs.Add(time.Since(t0).Nanoseconds())
		p.fetchN.Add(1)
		p.inFlight.Add(-1)

		p.cache.Put(hash, data)

		return data, nil
	})

	return v.([]byte)
}

func (p *Prefetcher) Stats() string {
	n := p.fetchN.Load()
	avgMs := 0.0

	if n > 0 {
		avgMs = float64(p.fetchNs.Load()) / float64(n) / 1e6
	}

	return fmt.Sprintf(
		"hits=%d misses=%d ranges=%d local-skip=%d fetched=%d inflight=%d/%d queue=%d/%d avg-fetch=%.1fms",
		p.hits.Load(), p.misses.Load(),
		p.submitted.Load(), p.skippedLocal.Load(),
		n, p.inFlight.Load(), p.workers,
		len(p.ranges), cap(p.ranges), avgMs)
}
