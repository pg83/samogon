package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

// samogonStorage plugs into anacrolix. One per fetch process. Pieces
// land in RAM until verified, then get uploaded to CAS and the buffer
// is dropped. Closed uploads are gated by a small semaphore so a
// completing torrent doesn't stampede mc processes.
type samogonStorage struct {
	cfg   *Config
	store *Storage
	sem   chan struct{}
}

func newSamogonStorage(cfg *Config, store *Storage) *samogonStorage {
	return &samogonStorage{
		cfg:   cfg,
		store: store,
		sem:   make(chan struct{}, cfg.UpSem),
	}
}

func (s *samogonStorage) OpenTorrent(ctx context.Context, info *metainfo.Info, infoHash metainfo.Hash) (storage.TorrentImpl, error) {
	pieceFn := func(p metainfo.Piece, pieceHash g.Option[[]byte]) storage.PieceImpl {
		if !pieceHash.Ok {
			// v2-only torrents don't surface a piece hash at
			// this layer — we'd need per-file Merkle trees to
			// build a CAS key. Out of scope for now.
			ThrowFmt("samogon: piece hash unavailable (pure v2 torrent?); only v1/hybrid supported")
		}

		return &samogonPiece{
			s:      s,
			hash:   hex.EncodeToString(pieceHash.Value),
			length: p.Length(),
		}
	}

	return storage.TorrentImpl{
		PieceWithHash: pieceFn,
		Close:         func() error { return nil },
	}, nil
}

func (s *samogonStorage) Close() error {
	return nil
}

type samogonPiece struct {
	s      *samogonStorage
	hash   string
	length int64

	mu       sync.Mutex
	buf      []byte
	complete bool
	checked  bool // we have HEAD-tested the CAS at least once
}

func (p *samogonPiece) ensureBuf() {
	if p.buf == nil {
		p.buf = make([]byte, p.length)
	}
}

func (p *samogonPiece) WriteAt(b []byte, off int64) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ensureBuf()

	end := off + int64(len(b))

	if end > int64(len(p.buf)) {
		return 0, io.ErrShortWrite
	}

	n := copy(p.buf[off:], b)

	return n, nil
}

func (p *samogonPiece) ReadAt(b []byte, off int64) (int, error) {
	p.mu.Lock()
	buf := p.buf
	complete := p.complete
	p.mu.Unlock()

	if buf != nil {
		if off >= int64(len(buf)) {
			return 0, io.EOF
		}

		n := copy(b, buf[off:])

		return n, nil
	}

	if complete {
		// Post-MarkComplete reads are rare in download-only mode
		// (Seed=false). Fall back to CAS for correctness if it
		// does happen.
		data := p.s.store.Cat(p.s.cfg.KeyPiece(p.hash))

		if off >= int64(len(data)) {
			return 0, io.EOF
		}

		n := copy(b, data[off:])

		return n, nil
	}

	return 0, io.EOF
}

func (p *samogonPiece) MarkComplete() error {
	p.mu.Lock()
	data := p.buf
	p.mu.Unlock()

	return runCatch(func() {
		p.s.sem <- struct{}{}
		defer func() { <-p.s.sem }()

		p.s.store.PutBytes(p.s.cfg.KeyPiece(p.hash), data)

		p.mu.Lock()
		p.complete = true
		p.buf = nil
		p.checked = true
		p.mu.Unlock()
	})
}

func (p *samogonPiece) MarkNotComplete() error {
	p.mu.Lock()
	p.complete = false
	p.buf = nil
	p.mu.Unlock()

	return nil
}

func (p *samogonPiece) Completion() storage.Completion {
	p.mu.Lock()
	complete := p.complete
	checked := p.checked
	p.mu.Unlock()

	if complete {
		return storage.Completion{Complete: true, Ok: true}
	}

	if checked {
		return storage.Completion{Complete: false, Ok: true}
	}

	// First call — consult CAS once to decide whether this piece
	// was uploaded by a previous run. A missed upload just means
	// the piece gets re-downloaded, which is fine.
	exists := p.s.store.Stat(p.s.cfg.KeyPiece(p.hash))

	p.mu.Lock()
	p.checked = true

	if exists {
		p.complete = true
	}

	p.mu.Unlock()

	return storage.Completion{Complete: exists, Ok: true}
}

// runCatch executes fn under Try and converts a thrown Exception into
// an error so it can be returned through anacrolix's error-returning
// interface without crashing the client.
func runCatch(fn func()) error {
	exc := Try(fn)

	if exc == nil {
		return nil
	}

	return exc.Unwrap()
}

func runFetch(cfg *Config) {
	// .torrent is read from stdin rather than argv — real torrents
	// routinely exceed ARG_MAX as base64 (ubuntu-25.10 desktop.iso
	// torrent is ~400KiB, well past the 128KiB default). Callers
	// pipe raw bytes in; gorn integration is a shell one-liner that
	// base64-decodes into the pipe from within the task script.
	raw := Throw2(io.ReadAll(os.Stdin))

	if len(raw) == 0 {
		ThrowFmt("fetch: empty stdin — pipe .torrent bytes in")
	}

	mi := Throw2(metainfo.Load(bytes.NewReader(raw)))
	infohash := mi.HashInfoBytes().HexString()

	store := newStorage(cfg)
	defer store.Close()

	if store.Stat(cfg.KeyTorrent(infohash)) {
		fmt.Fprintln(os.Stderr, clr(clrG, "fetch: already-done "+infohash))

		return
	}

	scratch := Throw2(os.MkdirTemp(".", "samogon-fetch-"))
	defer os.RemoveAll(scratch)

	tcfg := torrent.NewDefaultClientConfig()
	tcfg.DefaultStorage = newSamogonStorage(cfg, store)
	tcfg.DataDir = scratch
	tcfg.Seed = false

	client := Throw2(torrent.NewClient(tcfg))
	defer client.Close()

	t := Throw2(client.AddTorrent(mi))
	<-t.GotInfo()
	t.DownloadAll()

	done := make(chan struct{})

	go reportProgress(t, done)

	waited := client.WaitAll()

	close(done)

	if !waited {
		ThrowFmt("fetch: client.WaitAll returned false (torrent %s)", infohash)
	}

	// .torrent upload is the "commit" — do it last, after every
	// piece is in CAS. A crash before this point leaves orphan
	// pieces in CAS (harmless; next run resumes via Completion()).
	store.PutBytes(cfg.KeyTorrent(infohash), raw)

	fmt.Fprintln(os.Stderr, clr(clrG, "fetch: done "+infohash))
}

func reportProgress(t *torrent.Torrent, done <-chan struct{}) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-done:
			return

		case <-tick.C:
			total := t.Length()
			got := t.BytesCompleted()

			pct := 0.0

			if total > 0 {
				pct = 100.0 * float64(got) / float64(total)
			}

			fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf("fetch: %d/%d (%.1f%%)", got, total, pct)))
		}
	}
}
