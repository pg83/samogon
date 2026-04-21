package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/pkg/sftp"
)

type sftpHandlers struct {
	meta  *Meta
	store *Storage
	cache *LRU
	cfg   *Config
}

func newSftpHandlers(cfg *Config, meta *Meta, store *Storage, cache *LRU) sftp.Handlers {
	h := &sftpHandlers{cfg: cfg, meta: meta, store: store, cache: cache}

	return sftp.Handlers{
		FileGet:  h,
		FilePut:  h,
		FileCmd:  h,
		FileList: h,
	}
}

func (h *sftpHandlers) Fileread(r *sftp.Request) (io.ReaderAt, error) {
	var out io.ReaderAt

	exc := Try(func() {
		out = h.openFile(r.Filepath)
	})

	if exc != nil {
		return nil, exc.Unwrap()
	}

	return out, nil
}

func (h *sftpHandlers) Filewrite(r *sftp.Request) (io.WriterAt, error) {
	return nil, os.ErrPermission
}

func (h *sftpHandlers) Filecmd(r *sftp.Request) error {
	return os.ErrPermission
}

func (h *sftpHandlers) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	var out sftp.ListerAt

	exc := Try(func() {
		switch r.Method {
		case "List":
			out = h.list(r.Filepath)

		case "Stat":
			out = h.stat(r.Filepath)

		case "Readlink":
			ThrowFmt("readlink not supported: %s", r.Filepath)

		default:
			ThrowFmt("unknown Filelist method %q", r.Method)
		}
	})

	if exc != nil {
		return nil, exc.Unwrap()
	}

	return out, nil
}

// splitPath splits a POSIX path into (torrentName, fileSubpath). An
// empty torrentName means the root.
func splitPath(p string) (string, string) {
	p = path.Clean("/" + p)
	p = strings.TrimPrefix(p, "/")

	if p == "" || p == "." {
		return "", ""
	}

	if i := strings.Index(p, "/"); i >= 0 {
		return p[:i], p[i+1:]
	}

	return p, ""
}

func (h *sftpHandlers) list(p string) sftp.ListerAt {
	tname, fpath := splitPath(p)

	if tname == "" {
		names := h.meta.Names()
		sort.Strings(names)

		infos := make([]os.FileInfo, 0, len(names))

		for _, n := range names {
			infos = append(infos, newInfo(n, 0, true))
		}

		return listerAt(infos)
	}

	tm, ok := h.meta.ByName(tname)

	if !ok {
		ThrowFmt("no such torrent: %s", tname)
	}

	prefix := ""

	if fpath != "" {
		prefix = fpath + "/"
	}

	seenDir := map[string]bool{}
	out := []os.FileInfo{}

	for i := range tm.Files {
		f := &tm.Files[i]

		if !strings.HasPrefix(f.Path, prefix) {
			continue
		}

		rest := f.Path[len(prefix):]

		if rest == "" {
			continue
		}

		if idx := strings.Index(rest, "/"); idx >= 0 {
			dname := rest[:idx]

			if seenDir[dname] {
				continue
			}

			seenDir[dname] = true
			out = append(out, newInfo(dname, 0, true))

			continue
		}

		out = append(out, newInfo(rest, f.Size, false))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })

	return listerAt(out)
}

func (h *sftpHandlers) stat(p string) sftp.ListerAt {
	tname, fpath := splitPath(p)

	if tname == "" {
		return listerAt([]os.FileInfo{newInfo("/", 0, true)})
	}

	tm, ok := h.meta.ByName(tname)

	if !ok {
		ThrowFmt("no such torrent: %s", tname)
	}

	if fpath == "" {
		return listerAt([]os.FileInfo{newInfo(tname, tm.TotalLen, true)})
	}

	for i := range tm.Files {
		if tm.Files[i].Path == fpath {
			return listerAt([]os.FileInfo{newInfo(path.Base(fpath), tm.Files[i].Size, false)})
		}
	}

	prefix := fpath + "/"

	for i := range tm.Files {
		if strings.HasPrefix(tm.Files[i].Path, prefix) {
			return listerAt([]os.FileInfo{newInfo(path.Base(fpath), 0, true)})
		}
	}

	ThrowFmt("no such path: %s", p)

	return nil
}

func (h *sftpHandlers) openFile(p string) *virtualFile {
	tname, fpath := splitPath(p)

	if tname == "" || fpath == "" {
		ThrowFmt("not a file: %s", p)
	}

	tm, ok := h.meta.ByName(tname)

	if !ok {
		ThrowFmt("no such torrent: %s", tname)
	}

	for i := range tm.Files {
		if tm.Files[i].Path == fpath {
			return &virtualFile{
				tm:    tm,
				file:  &tm.Files[i],
				store: h.store,
				cache: h.cache,
				cfg:   h.cfg,
			}
		}
	}

	ThrowFmt("no such file: %s", p)

	return nil
}

type virtualFile struct {
	tm    *TorrentMeta
	file  *FileEntry
	store *Storage
	cache *LRU
	cfg   *Config
}

func (v *virtualFile) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}

	if off >= v.file.Size {
		return 0, io.EOF
	}

	// Clip request to file bounds so the piece walk can't run off
	// the end of the last file.
	remaining := v.file.Size - off

	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	streamOff := v.file.Offset + off
	pieceLen := v.tm.PieceLen
	total := 0

	for total < len(p) {
		pieceIdx := int(streamOff / pieceLen)

		if pieceIdx >= len(v.tm.PieceHashes) {
			break
		}

		pieceOff := int(streamOff % pieceLen)
		piece := v.getPiece(pieceIdx)

		if pieceOff >= len(piece) {
			break
		}

		n := copy(p[total:], piece[pieceOff:])

		if n == 0 {
			break
		}

		total += n
		streamOff += int64(n)
	}

	if int64(total) < remaining && total < len(p) {
		return total, io.EOF
	}

	if int64(off)+int64(total) >= v.file.Size {
		return total, io.EOF
	}

	return total, nil
}

func (v *virtualFile) getPiece(idx int) []byte {
	h := v.tm.PieceHashes[idx]

	if data, ok := v.cache.Get(h); ok {
		return data
	}

	data := v.store.Cat(v.cfg.KeyPiece(h))
	v.cache.Put(h, data)

	return data
}

type listerAt []os.FileInfo

func (l listerAt) ListAt(out []os.FileInfo, off int64) (int, error) {
	if off >= int64(len(l)) {
		return 0, io.EOF
	}

	n := copy(out, l[off:])

	if n < len(out) {
		return n, io.EOF
	}

	return n, nil
}

type vInfo struct {
	name string
	size int64
	dir  bool
	mt   time.Time
}

func newInfo(name string, size int64, dir bool) *vInfo {
	return &vInfo{name: name, size: size, dir: dir, mt: time.Unix(0, 0)}
}

func (i *vInfo) Name() string       { return i.name }
func (i *vInfo) Size() int64        { return i.size }
func (i *vInfo) ModTime() time.Time { return i.mt }
func (i *vInfo) IsDir() bool        { return i.dir }
func (i *vInfo) Sys() any           { return nil }

func (i *vInfo) Mode() os.FileMode {
	if i.dir {
		return os.ModeDir | 0o555
	}

	return 0o444
}
