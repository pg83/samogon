package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"
)

// get is a local test harness for the serve-side read path. It wires
// up the same Storage → LRU → Prefetcher → virtualFile chain that
// handleSession builds for SFTP clients, then streams a single file
// out to disk. Useful for measuring prefetcher behavior (cache
// hit/miss ratio, throughput, latency) without an SFTP client in
// the loop.

type getOpts struct {
	target  string
	outPath string
	chunk   int
}

func parseGetArgs(args []string) (*Config, getOpts) {
	c := loadCommon()

	fs := flag.NewFlagSet("samogon get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&c.AWSKey, "aws-key", c.AWSKey, "S3 access key (env AWS_ACCESS_KEY_ID)")
	fs.StringVar(&c.AWSSecret, "aws-secret", c.AWSSecret, "S3 secret key (env AWS_SECRET_ACCESS_KEY)")
	fs.StringVar(&c.S3Endpt, "endpoint", c.S3Endpt, "S3 endpoint URL (env S3_ENDPOINT)")
	fs.StringVar(&c.S3Bucket, "bucket", c.S3Bucket, "S3 bucket (env S3_BUCKET)")
	fs.StringVar(&c.S3Root, "s3-root", c.S3Root, "S3 key prefix (env SAMOGON_S3_ROOT)")
	fs.StringVar(&c.Region, "region", c.Region, "S3 region")
	fs.IntVar(&c.LRUSize, "lru", c.LRUSize, "piece cache size (entries)")
	fs.IntVar(&c.UpSem, "up-parallel", c.UpSem, "max concurrent GetObject calls")
	fs.IntVar(&c.PrefetchK, "prefetch-k", c.PrefetchK, "pieces to readahead on every getPiece (0 disables)")

	opts := getOpts{}

	fs.StringVar(&opts.outPath, "o", "", "output path (default: basename of target)")
	fs.IntVar(&opts.chunk, "chunk-size", 1<<20, "ReadAt chunk size in bytes")

	Throw(fs.Parse(args))

	if fs.NArg() != 1 {
		ThrowFmt("samogon get: expected one positional arg: <torrent>/<file>")
	}

	opts.target = fs.Arg(0)

	if opts.outPath == "" {
		opts.outPath = path.Base(strings.TrimRight(opts.target, "/"))
	}

	if opts.chunk <= 0 {
		ThrowFmt("--chunk-size must be > 0")
	}

	validate(c)

	return c, opts
}

func runGet(cfg *Config, opts getOpts) {
	store := newStorage(cfg)
	defer store.Close()

	cache := newLRU(cfg.LRUSize)
	meta := newMeta(cfg, store)
	pref := newPrefetcher(cfg, store, cache, cfg.UpSem)

	fmt.Fprintln(os.Stderr, clr(clrB, "get: loading meta"))
	meta.Reload()
	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf("get: known torrents: %d", len(meta.Names()))))

	tname, fpath := splitPath("/" + opts.target)

	if tname == "" {
		ThrowFmt("get: target must start with <torrent-name>, got %q", opts.target)
	}

	tm, ok := meta.ByName(tname)

	if !ok {
		ThrowFmt("get: no such torrent: %s (known: %v)", tname, meta.Names())
	}

	var file *FileEntry

	if fpath == "" {
		// Bare torrent name. For a single-file torrent (ISO etc.)
		// pick the only file; for multi-file, reject and list what's
		// available so the caller can pick.
		if len(tm.Files) == 1 {
			file = &tm.Files[0]
		} else {
			paths := make([]string, len(tm.Files))

			for i := range tm.Files {
				paths[i] = tm.Files[i].Path
			}

			ThrowFmt("get: torrent %q has %d files; specify one as %s/<path>; available: %v",
				tname, len(tm.Files), tname, paths)
		}
	} else {
		for i := range tm.Files {
			if tm.Files[i].Path == fpath {
				file = &tm.Files[i]

				break
			}
		}

		if file == nil {
			ThrowFmt("get: no such file in torrent %q: %s", tname, fpath)
		}
	}

	vf := &virtualFile{
		tm:   tm,
		file: file,
		pref: pref,
	}

	out := Throw2(os.Create(opts.outPath))
	defer out.Close()

	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf(
		"get: %s (%d bytes) → %s  chunk=%d  lru=%d  up-parallel=%d",
		opts.target, file.Size, opts.outPath, opts.chunk, cfg.LRUSize, cfg.UpSem)))

	var gotBytes atomic.Int64

	start := time.Now()
	done := make(chan struct{})

	go getReporter(&gotBytes, file.Size, pref, start, done)

	buf := make([]byte, opts.chunk)
	off := int64(0)

	for off < file.Size {
		want := int64(len(buf))

		if remaining := file.Size - off; want > remaining {
			want = remaining
		}

		n, err := vf.ReadAt(buf[:want], off)

		if n > 0 {
			_ = Throw2(out.Write(buf[:n]))
			off += int64(n)
			gotBytes.Store(off)
		}

		if err == io.EOF {
			if off < file.Size {
				ThrowFmt("get: short read at %d/%d", off, file.Size)
			}

			break
		}

		if err != nil {
			Throw(err)
		}
	}

	close(done)

	elapsed := time.Since(start)
	rate := float64(off) / elapsed.Seconds()

	fmt.Fprintln(os.Stderr, clr(clrG, fmt.Sprintf(
		"get: done — %d bytes in %s  %.2f MiB/s  [%s]",
		off, elapsed.Round(time.Millisecond),
		rate/(1024*1024), pref.Stats())))
}

func getReporter(bytes *atomic.Int64, total int64, pref *Prefetcher, start time.Time, done <-chan struct{}) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-done:
			return

		case now := <-tick.C:
			cur := bytes.Load()
			elapsed := now.Sub(start).Seconds()

			rate := 0.0

			if elapsed > 0 {
				rate = float64(cur) / elapsed
			}

			pct := 0.0

			if total > 0 {
				pct = 100.0 * float64(cur) / float64(total)
			}

			fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf(
				"get: %d/%d (%.1f%%)  %.2f MiB/s  %s",
				cur, total, pct, rate/(1024*1024), pref.Stats())))
		}
	}
}
