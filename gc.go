package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent/metainfo"
)

// gc walks every .torrent in <root>/torrents/, unions the V1 piece
// hashes referenced by them, then deletes any blob under
// <root>/pieces/ whose hash is not in the union.

func parseGcArgs(args []string) *Config {
	c := loadCommon()

	fs := flag.NewFlagSet("samogon gc", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&c.AWSKey, "aws-key", c.AWSKey, "S3 access key (env AWS_ACCESS_KEY_ID)")
	fs.StringVar(&c.AWSSecret, "aws-secret", c.AWSSecret, "S3 secret key (env AWS_SECRET_ACCESS_KEY)")
	fs.StringVar(&c.S3Endpt, "endpoint", c.S3Endpt, "S3 endpoint URL (env S3_ENDPOINT)")
	fs.StringVar(&c.S3Bucket, "bucket", c.S3Bucket, "S3 bucket (env S3_BUCKET)")
	fs.StringVar(&c.S3Root, "s3-root", c.S3Root, "S3 key prefix (env SAMOGON_S3_ROOT)")
	fs.StringVar(&c.Region, "region", c.Region, "S3 region")
	fs.IntVar(&c.UpSem, "max-inflight", c.UpSem, "worker count (== max concurrent S3 ops)")

	Throw(fs.Parse(args))

	if fs.NArg() > 0 {
		ThrowFmt("samogon gc: unexpected positional args: %v", fs.Args())
	}

	validate(c)

	return c
}

// pmap fans out `fn` over `items` across `workers` goroutines pulling
// from a single channel. Returns when every item has been processed.
func pmap[T any](items []T, workers int, fn func(T)) {
	jobs := make(chan T)

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := range jobs {
				fn(j)
			}
		}()
	}

	for _, it := range items {
		jobs <- it
	}

	close(jobs)
	wg.Wait()
}

func runGc(cfg *Config) {
	store := newStorage(cfg)
	defer store.Close()

	start := time.Now()

	torrentKeys := store.List(cfg.PrefixTorrents())

	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf("gc: %d torrents under %s", len(torrentKeys), cfg.PrefixTorrents())))

	var (
		alive   sync.Map
		loaded  atomic.Int64
		skipped atomic.Int64
	)

	pmap(torrentKeys, cfg.UpSem, func(key string) {
		exc := Try(func() {
			raw := store.Cat(key)
			mi := Throw2(metainfo.Load(bytes.NewReader(raw)))
			info := Throw2(mi.UnmarshalInfo())

			for i := 0; i < info.NumPieces(); i++ {
				h := info.Piece(i).V1Hash().Unwrap().HexString()
				alive.Store(h, struct{}{})
			}

			loaded.Add(1)
		})

		exc.Catch(func(e *Exception) {
			fmt.Fprintln(os.Stderr, clr(clrY, "gc: skip "+key+": "+e.Error()))
			skipped.Add(1)
		})
	})

	aliveCount := 0
	alive.Range(func(_, _ any) bool { aliveCount++; return true })

	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf("gc: loaded %d torrents (%d skipped), %d unique alive pieces",
		loaded.Load(), skipped.Load(), aliveCount)))

	pieceKeys := store.List(cfg.PrefixPieces())

	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf("gc: %d pieces in CAS", len(pieceKeys))))

	stale := pieceKeys[:0]

	for _, k := range pieceKeys {
		if _, ok := alive.Load(path.Base(k)); !ok {
			stale = append(stale, k)
		}
	}

	kept := len(pieceKeys) - len(stale)
	var deleted atomic.Int64

	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf("gc: %d kept, %d to delete", kept, len(stale))))

	pmap(stale, cfg.UpSem, func(key string) {
		store.Delete(key)
		deleted.Add(1)
	})

	elapsed := time.Since(start)

	fmt.Fprintln(os.Stderr, clr(clrG, fmt.Sprintf("gc: done — kept %d, deleted %d in %s",
		kept, deleted.Load(), elapsed.Round(time.Millisecond))))
}
