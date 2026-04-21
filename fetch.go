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
	alog "github.com/anacrolix/log"
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
	// known is the set of piece hashes already in CAS, loaded once
	// at fetch start via a single `mc ls pieces/`. Per-piece
	// Completion() checks this map instead of forking minio-client
	// per piece — 21k+ serial HEADs took anacrolix's AddTorrent off
	// the cliff before any download started.
	known map[string]bool
	// states holds per-piece-index buffers + completion flags.
	// PieceWithHash is called fresh on every anacrolix read/write
	// access (piece.go:88), so PieceImpl must not carry state of
	// its own — writes via one façade would land on a buffer a
	// read via the next façade can't see. All mutable state lives
	// here, keyed by piece index, and samogonPiece is just a thin
	// facade over the shared state.
	statesMu sync.Mutex
	states   map[int]*pieceState
}

type pieceState struct {
	hash   string
	length int64

	mu       sync.Mutex
	buf      []byte
	complete bool
}

func newSamogonStorage(cfg *Config, store *Storage, known map[string]bool) *samogonStorage {
	return &samogonStorage{
		cfg:    cfg,
		store:  store,
		sem:    make(chan struct{}, cfg.UpSem),
		known:  known,
		states: map[int]*pieceState{},
	}
}

func (s *samogonStorage) stateFor(idx int, hash string, length int64) *pieceState {
	s.statesMu.Lock()
	defer s.statesMu.Unlock()

	ps, ok := s.states[idx]

	if !ok {
		ps = &pieceState{hash: hash, length: length}
		s.states[idx] = ps
	}

	return ps
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
			s:  s,
			st: s.stateFor(p.Index(), hex.EncodeToString(pieceHash.Value), p.Length()),
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
	s  *samogonStorage
	st *pieceState
}

func (st *pieceState) ensureBuf() {
	if st.buf == nil {
		st.buf = make([]byte, st.length)
	}
}

func (p *samogonPiece) WriteAt(b []byte, off int64) (int, error) {
	p.st.mu.Lock()
	defer p.st.mu.Unlock()

	p.st.ensureBuf()

	end := off + int64(len(b))

	if end > int64(len(p.st.buf)) {
		return 0, io.ErrShortWrite
	}

	n := copy(p.st.buf[off:], b)

	return n, nil
}

func (p *samogonPiece) ReadAt(b []byte, off int64) (int, error) {
	p.st.mu.Lock()
	buf := p.st.buf
	complete := p.st.complete
	p.st.mu.Unlock()

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
		data := p.s.store.Cat(p.s.cfg.KeyPiece(p.st.hash))

		if off >= int64(len(data)) {
			return 0, io.EOF
		}

		n := copy(b, data[off:])

		return n, nil
	}

	return 0, io.EOF
}

func (p *samogonPiece) MarkComplete() error {
	p.st.mu.Lock()
	data := p.st.buf
	p.st.mu.Unlock()

	return runCatch(func() {
		p.s.sem <- struct{}{}
		defer func() { <-p.s.sem }()

		p.s.store.PutBytes(p.s.cfg.KeyPiece(p.st.hash), data)

		p.st.mu.Lock()
		p.st.complete = true
		p.st.buf = nil
		p.st.mu.Unlock()
	})
}

func (p *samogonPiece) MarkNotComplete() error {
	p.st.mu.Lock()
	p.st.complete = false
	p.st.buf = nil
	p.st.mu.Unlock()

	return nil
}

func (p *samogonPiece) Completion() storage.Completion {
	p.st.mu.Lock()
	complete := p.st.complete
	p.st.mu.Unlock()

	if complete {
		return storage.Completion{Complete: true, Ok: true}
	}

	// CAS lookup is in-memory — the hash set was populated once at
	// fetch start. If a previous run left the piece in S3 we pick
	// it up as complete; anacrolix then skips downloading it.
	if p.s.known[p.st.hash] {
		p.st.mu.Lock()
		p.st.complete = true
		p.st.mu.Unlock()

		return storage.Completion{Complete: true, Ok: true}
	}

	return storage.Completion{Complete: false, Ok: true}
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

func logf(color, format string, args ...any) {
	fmt.Fprintln(os.Stderr, clr(color, "fetch: "+fmt.Sprintf(format, args...)))
}

func runFetch(cfg *Config) {
	// .torrent is read from stdin rather than argv — real torrents
	// routinely exceed ARG_MAX as base64 (ubuntu-25.10 desktop.iso
	// torrent is ~400KiB, well past the 128KiB default). Callers
	// pipe raw bytes in; gorn integration is a shell one-liner that
	// base64-decodes into the pipe from within the task script.
	logf(clrB, "reading .torrent from stdin")
	raw := Throw2(io.ReadAll(os.Stdin))

	if len(raw) == 0 {
		ThrowFmt("fetch: empty stdin — pipe .torrent bytes in")
	}

	logf(clrB, "got %d bytes, parsing", len(raw))

	mi := Throw2(metainfo.Load(bytes.NewReader(raw)))
	info := Throw2(mi.UnmarshalInfo())
	infohash := mi.HashInfoBytes().HexString()

	logf(clrB, "infohash=%s name=%q pieces=%d total=%d bytes",
		infohash, info.Name, info.NumPieces(), info.TotalLength())

	store := newStorage(cfg)
	defer store.Close()

	logf(clrB, "checking S3 for commit marker %s", cfg.KeyTorrent(infohash))

	if store.Stat(cfg.KeyTorrent(infohash)) {
		logf(clrG, "already-done %s", infohash)

		return
	}

	scratch := Throw2(os.MkdirTemp(".", "samogon-fetch-"))
	defer os.RemoveAll(scratch)

	logf(clrB, "scratch dir %s", scratch)

	logf(clrB, "listing existing pieces in CAS")

	known := map[string]bool{}

	lsExc := Try(func() {
		known = store.ListPieces(cfg)
	})

	lsExc.Catch(func(e *Exception) {
		logf(clrY, "piece list failed (continuing with empty set): %v", e)
	})

	logf(clrB, "CAS has %d pieces already", len(known))

	tcfg := torrent.NewDefaultClientConfig()
	tcfg.DefaultStorage = newSamogonStorage(cfg, store, known)
	tcfg.DataDir = scratch
	tcfg.Seed = false
	// anacrolix defaults to 2 hashers per torrent, and MarkComplete
	// (= our S3 PutObject) is called synchronously from a hasher
	// goroutine. With a 2.7s average PutObject latency on a remote
	// MinIO, 32 hashers gave ~9 pieces/s — the tail drain after
	// download completion still took 30+ min on a 21k-piece torrent.
	// 128 pushes effective throughput proportional to concurrency
	// (minio handles this level of RPS trivially); memory cost is
	// ~piece_len × hashers, a handful of MiB at typical sizes.
	tcfg.PieceHashersPerTorrent = 128
	// Warning is anacrolix's default — raising to Info floods stderr
	// with per-piece / per-peer chatter that drowns our own progress
	// lines. Real problems (hash failures, tracker errors) come
	// through at Warning anyway.
	tcfg.Logger = alog.Default.FilterLevel(alog.Warning)

	logf(clrB, "starting anacrolix client")
	client := Throw2(torrent.NewClient(tcfg))
	defer client.Close()

	logf(clrB, "listen: %v", client.ListenAddrs())

	t := Throw2(client.AddTorrent(mi))
	logf(clrB, "torrent added, waiting for Info")
	<-t.GotInfo()
	logf(clrB, "Info ready, starting download")
	t.DownloadAll()

	done := make(chan struct{})

	st := tcfg.DefaultStorage.(*samogonStorage)

	go reportProgress(t, st, done)

	logf(clrB, "entering client.WaitAll — waits for every MarkComplete (upload) to finish")
	waited := client.WaitAll()
	logf(clrB, "client.WaitAll returned")

	close(done)

	if !waited {
		ThrowFmt("fetch: client.WaitAll returned false (torrent %s)", infohash)
	}

	// .torrent upload is the "commit" — do it last, after every
	// piece is in CAS. A crash before this point leaves orphan
	// pieces in CAS (harmless; next run resumes via Completion()).
	logf(clrB, "uploading commit marker")
	store.PutBytes(cfg.KeyTorrent(infohash), raw)

	logf(clrG, "done %s", infohash)
}

func reportProgress(t *torrent.Torrent, s *samogonStorage, done <-chan struct{}) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	var prevBytes int64

	prevT := time.Now()

	for {
		select {
		case <-done:
			return

		case now := <-tick.C:
			total := t.Length()
			got := t.BytesCompleted()
			ts := t.Stats()

			pct := 0.0

			if total > 0 {
				pct = 100.0 * float64(got) / float64(total)
			}

			dt := now.Sub(prevT).Seconds()
			rate := 0.0

			if dt > 0 {
				rate = float64(got-prevBytes) / dt
			}

			prevBytes = got
			prevT = now

			logf(clrB, "%d/%d (%.1f%%) %.1f KiB/s peers=%d active=%d pending=%d seeders=%d uploads=%d/%d puts=%d",
				got, total, pct, rate/1024.0,
				ts.TotalPeers, ts.ActivePeers, ts.PendingPeers, ts.ConnectedSeeders,
				len(s.sem), cap(s.sem), s.store.Puts.Load())
		}
	}
}
