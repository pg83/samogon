package main

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/anacrolix/torrent/metainfo"
)

type FileEntry struct {
	// Path as it should appear under the torrent's root directory.
	// Joined with "/" for anacrolix's []string Path.
	Path string
	Size int64
	// Byte offset in the virtual concatenation of all files (the
	// piece stream). A read at offset [Offset, Offset+Size) on the
	// stream is this file's bytes.
	Offset int64
}

type TorrentMeta struct {
	Infohash    string
	Name        string
	DisplayName string // byName key — unique across the root
	PieceLen    int64
	TotalLen    int64
	Files       []FileEntry
	PieceHashes []string // hex, index → CAS key suffix
}

type Meta struct {
	mu     sync.RWMutex
	cfg    *Config
	store  *Storage
	byName map[string]*TorrentMeta
	byHash map[string]*TorrentMeta
}

func newMeta(cfg *Config, store *Storage) *Meta {
	return &Meta{
		cfg:    cfg,
		store:  store,
		byName: map[string]*TorrentMeta{},
		byHash: map[string]*TorrentMeta{},
	}
}

func (m *Meta) Reload() {
	keys := m.store.List(m.cfg.PrefixTorrents())
	seen := map[string]bool{}

	for _, k := range keys {
		ih := path.Base(k)
		seen[ih] = true

		m.mu.RLock()
		_, already := m.byHash[ih]
		m.mu.RUnlock()

		if already {
			continue
		}

		exc := Try(func() {
			m.loadOne(ih)
		})

		exc.Catch(func(e *Exception) {
			fmt.Fprintln(os.Stderr, clr(clrY, "meta: skipping "+ih+": "+e.Error()))
		})
	}

	m.pruneGone(seen)
}

func (m *Meta) loadOne(infohash string) {
	data := m.store.Cat(m.cfg.KeyTorrent(infohash))
	mi := Throw2(metainfo.Load(bytes.NewReader(data)))
	info := Throw2(mi.UnmarshalInfo())

	tm := buildMeta(info, infohash)
	tm.DisplayName = m.uniqueName(tm.Name, tm.Infohash)

	m.mu.Lock()
	m.byHash[infohash] = tm
	m.byName[tm.DisplayName] = tm
	m.mu.Unlock()

	fmt.Fprintln(os.Stderr, clr(clrG, "meta: loaded "+tm.DisplayName+" ("+infohash+")"))
}

// uniqueName returns a display name unique under byName. Same-name
// torrents with different infohashes get a short suffix.
func (m *Meta) uniqueName(name, infohash string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, clash := m.byName[name]; !clash {
		return name
	}

	suffix := infohash

	if len(suffix) > 8 {
		suffix = suffix[:8]
	}

	return name + "-" + suffix
}

func (m *Meta) pruneGone(seen map[string]bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for ih, tm := range m.byHash {
		if seen[ih] {
			continue
		}

		delete(m.byHash, ih)
		delete(m.byName, tm.DisplayName)

		fmt.Fprintln(os.Stderr, clr(clrY, "meta: dropped "+tm.DisplayName+" ("+ih+")"))
	}
}

func (m *Meta) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]string, 0, len(m.byName))

	for n := range m.byName {
		out = append(out, n)
	}

	return out
}

func (m *Meta) ByName(name string) (*TorrentMeta, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tm, ok := m.byName[name]

	return tm, ok
}

func buildMeta(info metainfo.Info, infohash string) *TorrentMeta {
	tm := &TorrentMeta{
		Infohash: infohash,
		Name:     sanitizeName(info.Name),
		PieceLen: info.PieceLength,
		TotalLen: info.TotalLength(),
	}

	if len(info.Files) == 0 {
		tm.Files = []FileEntry{{
			Path:   tm.Name,
			Size:   info.Length,
			Offset: 0,
		}}
	} else {
		offset := int64(0)

		for _, f := range info.Files {
			tm.Files = append(tm.Files, FileEntry{
				Path:   strings.Join(f.Path, "/"),
				Size:   f.Length,
				Offset: offset,
			})
			offset += f.Length
		}
	}

	n := info.NumPieces()
	tm.PieceHashes = make([]string, n)

	for i := 0; i < n; i++ {
		// v1 hash; v2-only torrents aren't supported by samogon
		// right now (fetch rejects them at ingest time).
		tm.PieceHashes[i] = info.Piece(i).V1Hash().Unwrap().HexString()
	}

	return tm
}

// sanitizeName strips leading/trailing slashes and forbids "" or "." as
// a root name — SFTP clients will choke.
func sanitizeName(n string) string {
	n = strings.Trim(n, "/")

	if n == "" || n == "." || n == ".." {
		return "torrent"
	}

	return n
}
