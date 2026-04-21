package main

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Prefetcher runs background GetObjects into the LRU, following
// ranges the reader submits. Design points:
//
//   - Submit() is blocking. Natural backpressure: if workers can't
//     keep up, the reader stalls at submit instead of racing ahead.
//   - Queue items are ranges (slices of piece hashes), not individual
//     hashes — one channel send per reader step regardless of K.
//   - Workers process one range at a time sequentially. Concurrency
//     comes from N workers (= --up-parallel) pulling different
//     ranges in parallel.
//   - Dedup is claim-based, not singleflight. A worker reaching a
//     piece already claimed by someone else *skips*, rather than
//     waiting for the claim holder. This is critical: without it,
//     consecutive staggered ranges cause 32 workers to march in
//     lockstep through the frontier and collapse onto the same
//     leader, pinning inflight at 1. With skip-on-claim, workers
//     diverge onto different pieces — each successful claim results
//     in one real fetch, all others step forward to the next piece
//     in their own range.
//   - Foreground Get must wait for data, so it uses the claim map
//     slightly differently: if someone else has claimed, wait for
//     their chan to close (fetch completed), then read from cache.
//   - Each worker also keeps a local per-epoch set to avoid
//     re-entering the LRU / claim machinery for hashes it has
//     already processed in the last 100 ranges.
type Prefetcher struct {
	store *Storage
	cache *LRU
	cfg   *Config

	claim  sync.Map
	ranges chan []string

	workers int

	hits         atomic.Int64
	misses       atomic.Int64
	submitted    atomic.Int64 // ranges successfully enqueued
	skippedLocal atomic.Int64 // hashes skipped via worker local set
	skippedClaim atomic.Int64 // hashes skipped because another worker had them claimed
	fetchN       atomic.Int64 // unique GetObject calls
	fetchNs      atomic.Int64 // cumulative GetObject latency
	inFlight     atomic.Int64 // concurrent GetObject calls
}

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

// Submit enqueues a range of piece hashes. Blocks if queue is full.
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

			p.workerFetch(h)
		}

		processed++

		if processed >= workerEpoch {
			seen = make(map[string]struct{})
			processed = 0
		}
	}
}

// workerFetch warms the cache with hash, skipping if the piece is
// already in cache or currently claimed by another worker.
func (p *Prefetcher) workerFetch(hash string) {
	if _, ok := p.cache.Get(hash); ok {
		return
	}

	ch := make(chan struct{})

	if _, loaded := p.claim.LoadOrStore(hash, ch); loaded {
		// Someone else is fetching. Don't wait — move on.
		p.skippedClaim.Add(1)

		return
	}

	// We own the claim. Make sure to release it whatever happens.
	defer func() {
		p.claim.Delete(hash)
		close(ch)
	}()

	exc := Try(func() {
		p.doFetch(hash)
	})

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, clr(clrY, "prefetch: "+hash+": "+e.Error()))
	})
}

// Get is the foreground read path. If another goroutine is currently
// fetching, wait for it; otherwise fetch ourselves.
func (p *Prefetcher) Get(hash string) []byte {
	if data, ok := p.cache.Get(hash); ok {
		p.hits.Add(1)

		return data
	}

	p.misses.Add(1)

	ch := make(chan struct{})

	actual, loaded := p.claim.LoadOrStore(hash, ch)

	if loaded {
		// Somebody's on it. Wait for their fetch to complete,
		// then pull from cache.
		<-actual.(chan struct{})

		data, ok := p.cache.Get(hash)

		if !ok {
			// Leader's fetch errored (they deleted the claim
			// without populating cache). Do it ourselves.
			return p.fetchClaimed(hash)
		}

		return data
	}

	// We own the claim.
	defer func() {
		p.claim.Delete(hash)
		close(ch)
	}()

	return p.doFetch(hash)
}

// fetchClaimed is the fallback when a previous claim holder failed.
// Re-enter the claim dance fresh.
func (p *Prefetcher) fetchClaimed(hash string) []byte {
	ch := make(chan struct{})

	actual, loaded := p.claim.LoadOrStore(hash, ch)

	if loaded {
		<-actual.(chan struct{})

		data, ok := p.cache.Get(hash)

		if !ok {
			ThrowFmt("prefetch: repeated fetch of %s failed", hash)
		}

		return data
	}

	defer func() {
		p.claim.Delete(hash)
		close(ch)
	}()

	return p.doFetch(hash)
}

// doFetch assumes caller owns the claim; does the actual GetObject
// + cache population.
func (p *Prefetcher) doFetch(hash string) []byte {
	p.inFlight.Add(1)
	t0 := time.Now()

	data := p.store.Cat(p.cfg.KeyPiece(hash))

	p.fetchNs.Add(time.Since(t0).Nanoseconds())
	p.fetchN.Add(1)
	p.inFlight.Add(-1)

	p.cache.Put(hash, data)

	return data
}

func (p *Prefetcher) Stats() string {
	n := p.fetchN.Load()
	avgMs := 0.0

	if n > 0 {
		avgMs = float64(p.fetchNs.Load()) / float64(n) / 1e6
	}

	return fmt.Sprintf(
		"hits=%d misses=%d ranges=%d local-skip=%d claim-skip=%d fetched=%d inflight=%d/%d queue=%d/%d avg-fetch=%.1fms",
		p.hits.Load(), p.misses.Load(),
		p.submitted.Load(), p.skippedLocal.Load(), p.skippedClaim.Load(),
		n, p.inFlight.Load(), p.workers,
		len(p.ranges), cap(p.ranges), avgMs)
}
