package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// get2 is a bare-bones raw-read benchmark: grab every piece hash a
// file covers, feed them into a channel, let N workers pull and
// GetObject them in arbitrary order — no LRU, no prefetcher, no
// virtualFile, no disk. What we measure here is exactly "how fast
// can minio serve N concurrent GetObjects on real piece-keys".
//
// If get2 shows stable throughput at 32 parallel, the degradation
// in `get` is something in our pipeline (prefetcher, LRU, disk).
// If get2 itself slows down over time, the endpoint/network is the
// limit.

type get2Opts struct {
	target   string
	parallel int
}

func parseGet2Args(args []string) (*Config, get2Opts) {
	c := loadCommon()

	fs := flag.NewFlagSet("samogon get2", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&c.AWSKey, "aws-key", c.AWSKey, "S3 access key (env AWS_ACCESS_KEY_ID)")
	fs.StringVar(&c.AWSSecret, "aws-secret", c.AWSSecret, "S3 secret key (env AWS_SECRET_ACCESS_KEY)")
	fs.StringVar(&c.S3Endpt, "endpoint", c.S3Endpt, "S3 endpoint URL (env S3_ENDPOINT)")
	fs.StringVar(&c.S3Bucket, "bucket", c.S3Bucket, "S3 bucket (env S3_BUCKET)")
	fs.StringVar(&c.S3Root, "s3-root", c.S3Root, "S3 key prefix (env SAMOGON_S3_ROOT)")
	fs.StringVar(&c.Region, "region", c.Region, "S3 region")

	opts := get2Opts{}

	fs.IntVar(&opts.parallel, "parallel", 64, "number of worker goroutines")

	Throw(fs.Parse(args))

	if fs.NArg() != 1 {
		ThrowFmt("samogon get2: expected one positional arg: <torrent>[/<file>]")
	}

	opts.target = fs.Arg(0)

	if opts.parallel < 1 {
		ThrowFmt("--parallel must be >= 1")
	}

	validate(c)

	return c, opts
}

func runGet2(cfg *Config, opts get2Opts) {
	store := newStorage(cfg)
	defer store.Close()

	cache := newLRU(1) // minimal; we don't cache
	meta := newMeta(cfg, store)

	fmt.Fprintln(os.Stderr, clr(clrB, "get2: loading meta"))
	meta.Reload()

	_ = cache

	hashes := resolveHashes(cfg, meta, opts.target)

	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf(
		"get2: %s → %d pieces, parallel=%d, endpoint=%s",
		opts.target, len(hashes), opts.parallel, cfg.S3Endpt)))

	queue := make(chan string, len(hashes))

	for _, h := range hashes {
		queue <- h
	}

	close(queue)

	var ops atomic.Int64
	var bytesFetched atomic.Int64
	var latencyNs atomic.Int64
	var inFlight atomic.Int64
	var errs atomic.Int64

	start := time.Now()
	done := make(chan struct{})

	go get2Reporter(&ops, &bytesFetched, &latencyNs, &inFlight, &errs, int64(len(hashes)), start, done)

	var wg sync.WaitGroup

	for i := 0; i < opts.parallel; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for h := range queue {
				inFlight.Add(1)
				t0 := time.Now()

				n, err := get2Fetch(store, cfg, h)

				latencyNs.Add(time.Since(t0).Nanoseconds())
				inFlight.Add(-1)

				if err != nil {
					errs.Add(1)
					fmt.Fprintln(os.Stderr, clr(clrY, fmt.Sprintf("get2: %s: %v", h, err)))

					continue
				}

				bytesFetched.Add(int64(n))
				ops.Add(1)
			}
		}()
	}

	wg.Wait()
	close(done)

	elapsed := time.Since(start)
	finalOps := ops.Load()
	finalBytes := bytesFetched.Load()

	if finalOps == 0 {
		fmt.Fprintln(os.Stderr, clr(clrR, fmt.Sprintf(
			"get2: no ops completed in %s (errs=%d)", elapsed, errs.Load())))

		return
	}

	avgRate := float64(finalOps) / elapsed.Seconds()
	avgMBps := float64(finalBytes) / elapsed.Seconds() / (1024 * 1024)
	avgLatMs := float64(latencyNs.Load()) / float64(finalOps) / 1e6

	fmt.Fprintln(os.Stderr, clr(clrG, fmt.Sprintf(
		"get2: done — %d ops, %d bytes in %s  %.1f ops/s  %.2f MiB/s  %.1f ms avg-latency  errs=%d",
		finalOps, finalBytes, elapsed.Round(time.Millisecond),
		avgRate, avgMBps, avgLatMs, errs.Load())))
}

// get2Fetch does GetObject for a piece hash and drains the body
// without allocating or caching. Returns bytes read.
func get2Fetch(store *Storage, cfg *Config, hash string) (int64, error) {
	out, err := store.cli.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(cfg.KeyPiece(hash)),
	})

	if err != nil {
		return 0, err
	}

	defer out.Body.Close()

	// io.Copy to Discard — no allocation of a full buffer, just
	// stream through. Matches the network cost without the cache
	// allocation cost.
	n, err := io.Copy(io.Discard, out.Body)

	return n, err
}

func resolveHashes(cfg *Config, meta *Meta, target string) []string {
	tname, fpath := splitPath("/" + target)

	if tname == "" {
		ThrowFmt("get2: target must start with <torrent-name>")
	}

	tm, ok := meta.ByName(tname)

	if !ok {
		ThrowFmt("get2: no such torrent: %s", tname)
	}

	if fpath == "" {
		// Whole torrent.
		return append([]string{}, tm.PieceHashes...)
	}

	var file *FileEntry

	for i := range tm.Files {
		if tm.Files[i].Path == fpath {
			file = &tm.Files[i]

			break
		}
	}

	if file == nil {
		ThrowFmt("get2: no such file in torrent %q: %s", tname, fpath)
	}

	startPiece := file.Offset / tm.PieceLen
	endPiece := (file.Offset + file.Size - 1) / tm.PieceLen

	out := make([]string, 0, endPiece-startPiece+1)

	for i := startPiece; i <= endPiece; i++ {
		out = append(out, tm.PieceHashes[i])
	}

	return out
}

func get2Reporter(
	ops, bytesFetched, latencyNs, inFlight, errs *atomic.Int64,
	total int64, start time.Time, done <-chan struct{},
) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-done:
			return

		case now := <-tick.C:
			cur := ops.Load()
			curBytes := bytesFetched.Load()

			elapsed := now.Sub(start).Seconds()
			rate := 0.0
			mbps := 0.0

			if elapsed > 0 {
				rate = float64(cur) / elapsed
				mbps = float64(curBytes) / elapsed / (1024 * 1024)
			}

			avgLatMs := 0.0

			if cur > 0 {
				avgLatMs = float64(latencyNs.Load()) / float64(cur) / 1e6
			}

			fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf(
				"get2: ops=%d/%d errs=%d  %.1f ops/s  %.2f MiB/s  %.1f ms avg-latency  inflight=%d",
				cur, total, errs.Load(), rate, mbps, avgLatMs, inFlight.Load())))
		}
	}
}

// ensure imports are used — bytes is referenced elsewhere, keep here
// so a future tweak won't silently break imports.
var _ = bytes.Buffer{}
