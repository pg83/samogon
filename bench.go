package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// bench is a raw S3 PutObject stress tester. N goroutines spam
// `<root>/bench/<goroutine>.<chunk>` with a fixed-size payload; we
// report ops/s, throughput, and average per-request latency every 2s
// and at the end. Use it to pick sane defaults for
// PieceHashersPerTorrent / UpSem on a given minio endpoint without
// having to actually run a torrent download.

type benchOpts struct {
	parallel  int
	chunkSize int
	duration  time.Duration
}

func parseBenchArgs(args []string) (*Config, benchOpts) {
	c := loadCommon()

	fs := flag.NewFlagSet("samogon bench", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&c.AWSKey, "aws-key", c.AWSKey, "S3 access key (env AWS_ACCESS_KEY_ID)")
	fs.StringVar(&c.AWSSecret, "aws-secret", c.AWSSecret, "S3 secret key (env AWS_SECRET_ACCESS_KEY)")
	fs.StringVar(&c.S3Endpt, "endpoint", c.S3Endpt, "S3 endpoint URL (env S3_ENDPOINT)")
	fs.StringVar(&c.S3Bucket, "bucket", c.S3Bucket, "S3 bucket (env S3_BUCKET)")
	fs.StringVar(&c.S3Root, "s3-root", c.S3Root, "S3 key prefix (env SAMOGON_S3_ROOT)")
	fs.StringVar(&c.Region, "region", c.Region, "S3 region")

	opts := benchOpts{}

	fs.IntVar(&opts.parallel, "parallel", 32, "number of writer goroutines")
	fs.IntVar(&opts.chunkSize, "chunk-size", 262144, "bytes per PutObject")
	fs.DurationVar(&opts.duration, "duration", 30*time.Second, "how long to run")

	Throw(fs.Parse(args))

	if fs.NArg() > 0 {
		ThrowFmt("samogon bench: unexpected positional args: %v", fs.Args())
	}

	if opts.parallel < 1 {
		ThrowFmt("--parallel must be >= 1")
	}

	if opts.chunkSize < 0 {
		ThrowFmt("--chunk-size must be >= 0")
	}

	if opts.duration <= 0 {
		ThrowFmt("--duration must be > 0")
	}

	validate(c)

	return c, opts
}

func runBench(cfg *Config, opts benchOpts) {
	store := newStorage(cfg)
	defer store.Close()

	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf(
		"bench: parallel=%d chunk-size=%d duration=%s endpoint=%s bucket=%s root=%s",
		opts.parallel, opts.chunkSize, opts.duration,
		cfg.S3Endpt, cfg.S3Bucket, cfg.S3Root)))

	// One shared payload — PutObject reads; no mutation.
	payload := bytes.Repeat([]byte{'x'}, opts.chunkSize)

	var ops atomic.Int64
	var totalLatencyNs atomic.Int64
	var errs atomic.Int64

	start := time.Now()
	deadline := start.Add(opts.duration)
	done := make(chan struct{})

	go benchReporter(&ops, &totalLatencyNs, &errs, opts.chunkSize, start, done)

	var wg sync.WaitGroup

	for g := 0; g < opts.parallel; g++ {
		wg.Add(1)

		go func(gid int) {
			defer wg.Done()

			benchWorker(store, cfg, gid, payload, deadline, &ops, &totalLatencyNs, &errs)
		}(g)
	}

	wg.Wait()
	close(done)

	elapsed := time.Since(start)
	finalOps := ops.Load()

	if finalOps == 0 {
		fmt.Fprintln(os.Stderr, clr(clrR, fmt.Sprintf(
			"bench: no ops completed in %s (errors=%d)", elapsed, errs.Load())))

		return
	}

	avgRate := float64(finalOps) / elapsed.Seconds()
	avgMBps := avgRate * float64(opts.chunkSize) / (1024 * 1024)
	avgLatMs := float64(totalLatencyNs.Load()) / float64(finalOps) / 1e6

	fmt.Fprintln(os.Stderr, clr(clrG, fmt.Sprintf(
		"bench: done in %s — ops=%d errs=%d  %.1f ops/s  %.2f MiB/s  %.1f ms avg-latency",
		elapsed.Round(time.Millisecond), finalOps, errs.Load(),
		avgRate, avgMBps, avgLatMs)))
}

func benchWorker(
	store *Storage, cfg *Config, gid int, payload []byte, deadline time.Time,
	ops, totalLatencyNs, errs *atomic.Int64,
) {
	m := 0

	for time.Now().Before(deadline) {
		key := fmt.Sprintf("%s/bench/%d.%d", cfg.S3Root, gid, m)

		t0 := time.Now()

		_, err := store.cli.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket:        aws.String(cfg.S3Bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(payload),
			ContentLength: aws.Int64(int64(len(payload))),
		})

		lat := time.Since(t0)

		if err != nil {
			errs.Add(1)

			// Throttle error spam — one per goroutine per second
			// is enough to show the failure mode without drowning
			// the report.
			fmt.Fprintln(os.Stderr, clr(clrY, fmt.Sprintf(
				"bench: goroutine %d chunk %d: %v", gid, m, err)))

			time.Sleep(time.Second)
			m++

			continue
		}

		totalLatencyNs.Add(lat.Nanoseconds())
		ops.Add(1)
		m++
	}
}

func benchReporter(
	ops, totalLatencyNs, errs *atomic.Int64,
	chunkSize int, start time.Time, done <-chan struct{},
) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	var prevOps int64
	prevT := start

	for {
		select {
		case <-done:
			return

		case now := <-tick.C:
			curOps := ops.Load()

			dt := now.Sub(prevT).Seconds()
			rate := 0.0

			if dt > 0 {
				rate = float64(curOps-prevOps) / dt
			}

			mbps := rate * float64(chunkSize) / (1024 * 1024)

			avgLatMs := 0.0

			if curOps > 0 {
				avgLatMs = float64(totalLatencyNs.Load()) / float64(curOps) / 1e6
			}

			fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf(
				"bench: ops=%d errs=%d  %.1f ops/s  %.2f MiB/s  %.1f ms avg-latency",
				curOps, errs.Load(), rate, mbps, avgLatMs)))

			prevOps = curOps
			prevT = now
		}
	}
}
