package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"golang.org/x/sync/singleflight"
)

// repack rewrites an existing torrent's piece layout: streams the
// original piece sequence in order through the same Storage / LRU /
// singleflight chain that serve uses for SFTP, slices the byte stream
// into new pieces of `--piece-size`, hashes each, uploads to S3 as
// torrents/pieces/<sha1>, and writes the rebuilt .torrent under the
// new infohash.
//
// Motivation: many small pieces blow up S3 object count and amplify
// LIST cost. A 200 GiB torrent with 256 KiB pieces is 800k objects;
// at 4 MiB pieces it's 50k.
//
// The new torrent has a different infohash — swarm sharing of the
// repacked variant is a separate exercise; this is purely a
// CAS-side reorganisation.

type repackOpts struct {
	target    string
	pieceSize int64
}

func parseRepackArgs(args []string) (*Config, repackOpts) {
	c := loadCommon()

	fs := flag.NewFlagSet("samogon repack", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&c.AWSKey, "aws-key", c.AWSKey, "S3 access key (env AWS_ACCESS_KEY_ID)")
	fs.StringVar(&c.AWSSecret, "aws-secret", c.AWSSecret, "S3 secret key (env AWS_SECRET_ACCESS_KEY)")
	fs.StringVar(&c.S3Endpt, "endpoint", c.S3Endpt, "S3 endpoint URL (env S3_ENDPOINT)")
	fs.StringVar(&c.S3Bucket, "bucket", c.S3Bucket, "S3 bucket (env S3_BUCKET)")
	fs.StringVar(&c.S3Root, "s3-root", c.S3Root, "S3 key prefix (env SAMOGON_S3_ROOT)")
	fs.StringVar(&c.Region, "region", c.Region, "S3 region")
	fs.IntVar(&c.LRUSize, "lru", c.LRUSize, "piece cache size (entries)")
	fs.IntVar(&c.PrefetchDistance, "prefetch-distance", c.PrefetchDistance, "old pieces to readahead while streaming (0 disables)")

	opts := repackOpts{}

	fs.Int64Var(&opts.pieceSize, "piece-size", 4<<20, "new piece length in bytes")

	Throw(fs.Parse(args))

	if fs.NArg() != 1 {
		ThrowFmt("samogon repack: expected one positional arg: <torrent-key-or-infohash>")
	}

	opts.target = fs.Arg(0)

	if opts.pieceSize <= 0 {
		ThrowFmt("--piece-size must be > 0")
	}

	validate(c)

	return c, opts
}

// extractInfohash accepts an mc-style key like
//   /samogon/torrents/torrents/5e3e284f…c7
// or just
//   torrents/torrents/5e3e284f…c7
// or the bare 40-hex infohash. Trailing slashes ignored.
func extractInfohash(p string) string {
	p = strings.TrimRight(p, "/")
	p = path.Base(p)

	if len(p) != 40 {
		ThrowFmt("repack: expected 40-char hex infohash at end of path, got %q", p)
	}

	if _, err := hex.DecodeString(p); err != nil {
		ThrowFmt("repack: not a hex infohash: %s (%v)", p, err)
	}

	return p
}

func runRepack(cfg *Config, opts repackOpts) {
	infohash := extractInfohash(opts.target)

	store := newStorage(cfg)
	defer store.Close()

	cache := newLRU(cfg.LRUSize)

	raw := store.Cat(cfg.KeyTorrent(infohash))
	mi := Throw2(metainfo.Load(bytes.NewReader(raw)))
	info := Throw2(mi.UnmarshalInfo())

	loadedHash := mi.HashInfoBytes().HexString()

	if loadedHash != infohash {
		ThrowFmt("repack: loaded torrent infohash mismatch: %s != %s", loadedHash, infohash)
	}

	tm := buildMeta(info, infohash)

	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf(
		"repack: source infohash=%s name=%q pieces=%d totalLen=%d pieceLen=%d",
		infohash, info.Name, info.NumPieces(), info.TotalLength(), info.PieceLength)))
	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf(
		"repack: target pieceLen=%d (= %.2f MiB)",
		opts.pieceSize, float64(opts.pieceSize)/(1024*1024))))

	src := &pieceStream{
		hashes: tm.PieceHashes,
		cfg:    cfg,
		store:  store,
		cache:  cache,
		group:  &singleflight.Group{},
	}

	buf := make([]byte, opts.pieceSize)
	newPieces := make([]byte, 0, sha1.Size*((info.TotalLength()+opts.pieceSize-1)/opts.pieceSize))

	var (
		pieceCount int
		bytesRead  atomic.Int64
		newPuts    int
		skipPuts   int
	)

	start := time.Now()
	done := make(chan struct{})

	go repackReporter(&bytesRead, info.TotalLength(), start, done)

	for {
		n, err := io.ReadFull(src, buf)

		if n > 0 {
			chunk := buf[:n]
			h := sha1.Sum(chunk)
			hashHex := hex.EncodeToString(h[:])
			key := cfg.KeyPiece(hashHex)

			if store.Stat(key) {
				skipPuts++
			} else {
				store.PutBytes(key, chunk)
				newPuts++
			}

			newPieces = append(newPieces, h[:]...)
			pieceCount++
			bytesRead.Add(int64(n))
		}

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}

		if err != nil {
			Throw(err)
		}
	}

	close(done)

	if bytesRead.Load() != info.TotalLength() {
		ThrowFmt("repack: streamed %d bytes but expected %d", bytesRead.Load(), info.TotalLength())
	}

	elapsed := time.Since(start)

	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf(
		"repack: streamed %d bytes into %d new pieces in %s (%.2f MiB/s); %d new puts, %d skipped (already in CAS)",
		bytesRead.Load(), pieceCount, elapsed.Round(time.Millisecond),
		float64(bytesRead.Load())/elapsed.Seconds()/(1024*1024),
		newPuts, skipPuts)))

	// Rebuild info with new piece layout. PieceLength + Pieces are the
	// only fields that change; Name, Files, Private, etc. preserved.
	info.PieceLength = opts.pieceSize
	info.Pieces = newPieces

	infoBytes := Throw2(bencode.Marshal(info))
	mi.InfoBytes = infoBytes
	newInfohash := mi.HashInfoBytes().HexString()

	var out bytes.Buffer
	Throw(mi.Write(&out))

	rawNew := out.Bytes()

	fmt.Fprintln(os.Stderr, clr(clrG, fmt.Sprintf(
		"repack: new infohash=%s torrent_size=%d", newInfohash, len(rawNew))))

	if newInfohash == infohash {
		// Same piece layout in & out — nothing changed.
		fmt.Fprintln(os.Stderr, clr(clrY, "repack: source already at requested layout — nothing to do"))

		return
	}

	store.PutBytes(cfg.KeyTorrent(newInfohash), rawNew)

	fmt.Fprintln(os.Stderr, clr(clrG, fmt.Sprintf(
		"repack: wrote new torrent to %s", cfg.KeyTorrent(newInfohash))))
}

// pieceStream is an io.Reader over the source torrent's linear piece
// stream. Mirrors the cache+singleflight+prefetch shape from sftp.go's
// getPiece so we get the same readahead pipeline that SFTP uses.
type pieceStream struct {
	hashes []string
	cfg    *Config
	store  *Storage
	cache  *LRU
	group  *singleflight.Group

	cur int    // index of current piece
	pos int    // byte offset within current piece
	buf []byte // current piece bytes (nil = need to fetch)
}

func (s *pieceStream) Read(p []byte) (int, error) {
	n := 0

	for n < len(p) {
		if s.cur >= len(s.hashes) {
			if n > 0 {
				return n, nil
			}

			return 0, io.EOF
		}

		if s.buf == nil {
			s.buf = s.fetch(s.cur)
			s.pos = 0
		}

		c := copy(p[n:], s.buf[s.pos:])
		n += c
		s.pos += c

		if s.pos >= len(s.buf) {
			s.cur++
			s.buf = nil
		}
	}

	return n, nil
}

// fetch grabs piece `idx` through cache + singleflight. Same pattern
// as virtualFile.getPiece, with prefetch firing for `idx + PrefetchDistance`.
func (s *pieceStream) fetch(idx int) []byte {
	if d := s.cfg.PrefetchDistance; d > 0 && idx+d < len(s.hashes) {
		hash := s.hashes[idx+d]

		go func() {
			_, _, _ = s.group.Do(hash, func() (any, error) {
				if _, ok := s.cache.Get(hash); ok {
					return nil, nil
				}

				_ = Try(func() {
					s.cache.Put(hash, s.store.Cat(s.cfg.KeyPiece(hash)))
				})

				return nil, nil
			})
		}()
	}

	hash := s.hashes[idx]

	if data, ok := s.cache.Get(hash); ok {
		return data
	}

	v, _, _ := s.group.Do(hash, func() (any, error) {
		if data, ok := s.cache.Get(hash); ok {
			return data, nil
		}

		data := s.store.Cat(s.cfg.KeyPiece(hash))
		s.cache.Put(hash, data)

		return data, nil
	})

	return v.([]byte)
}

func repackReporter(bytesRead *atomic.Int64, total int64, start time.Time, done <-chan struct{}) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-done:
			return

		case now := <-tick.C:
			cur := bytesRead.Load()
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
				"repack: %d/%d (%.1f%%)  %.2f MiB/s",
				cur, total, pct, rate/(1024*1024))))
		}
	}
}
