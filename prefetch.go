package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// Prefetcher sits in front of Storage.Cat for SFTP reads. Two jobs:
//
//   - Dedupe concurrent fetches of the same piece. pkg/sftp fires
//     many parallel ReadAt requests against one file; without
//     singleflight, every one of them can race to GetObject on a cold
//     cache entry.
//   - Readahead. On each Get we async-fire fetches for the next K
//     pieces of the same torrent so a sequential streamer (video
//     playback, curl, sftp get) finds them already warm in the LRU.
//     Cache hits cost ~0. Misses on the critical path block the
//     reader — the whole point of this is to not block there.
type Prefetcher struct {
	store *Storage
	cache *LRU
	cfg   *Config

	sf  singleflight.Group
	sem chan struct{}

	hits      atomic.Int64
	misses    atomic.Int64
	preIss    atomic.Int64
	preHit    atomic.Int64
	fetchN    atomic.Int64 // completed GetObject calls (unique, via singleflight)
	fetchNs   atomic.Int64 // cumulative GetObject latency in ns
	inFlight  atomic.Int64 // concurrent GetObject calls in progress
}

func newPrefetcher(cfg *Config, store *Storage, cache *LRU, concurrency int) *Prefetcher {
	return &Prefetcher{
		store: store,
		cache: cache,
		cfg:   cfg,
		sem:   make(chan struct{}, concurrency),
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
		// Re-check the cache inside the flight — another caller may
		// have finished a fetch and populated it while we were
		// waiting for the lock.
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

	p.misses.Add(1)

	return v.([]byte)
}

// Prefetch kicks off async fetches for hashes not yet in cache. If a
// piece is already in-flight via singleflight, Do() collapses onto
// the existing fetch and we just wait for it (cheap). The
// concurrency bound keeps GetObject parallelism sane.
func (p *Prefetcher) Prefetch(hashes []string) {
	for _, h := range hashes {
		if _, ok := p.cache.Get(h); ok {
			p.preHit.Add(1)

			continue
		}

		p.preIss.Add(1)
		h := h

		go func() {
			p.sem <- struct{}{}
			defer func() { <-p.sem }()

			// Wrap in Try so a throw (S3 4xx, timeout) doesn't
			// kill the daemon. A prefetch miss just means the
			// caller's own Get will re-try synchronously and
			// surface the real error then.
			exc := Try(func() {
				p.Get(h)
			})

			exc.Catch(func(e *Exception) {
				fmt.Fprintln(os.Stderr, clr(clrY, "prefetch: "+h+": "+e.Error()))
			})
		}()
	}
}

func (p *Prefetcher) Stats() string {
	n := p.fetchN.Load()
	avgMs := 0.0

	if n > 0 {
		avgMs = float64(p.fetchNs.Load()) / float64(n) / 1e6
	}

	return fmt.Sprintf(
		"hits=%d misses=%d prefetch-issued=%d prefetch-hit=%d fetched=%d inflight=%d/%d avg-fetch=%.1fms",
		p.hits.Load(), p.misses.Load(),
		p.preIss.Load(), p.preHit.Load(),
		n, p.inFlight.Load(), cap(p.sem), avgMs)
}
