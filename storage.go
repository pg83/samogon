package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"
)

// Storage is the minio-client wrapper. One per process; anchors a mc
// config dir under cwd because the ci user in prod has neither a
// writable $HOME nor /tmp (see molot's history — same lesson learned
// the hard way). MC_HOST_samogon is passed via env on every invocation
// so we never write credentials to disk.
type Storage struct {
	cfg *Config
	dir string
}

func newStorage(cfg *Config) *Storage {
	dir := Throw2(os.MkdirTemp(".", "mc-samogon-"))

	return &Storage{cfg: cfg, dir: dir}
}

func (s *Storage) Close() {
	_ = os.RemoveAll(s.dir)
}

func (s *Storage) mc(args ...string) *exec.Cmd {
	all := append([]string{"--config-dir", s.dir}, args...)
	cmd := exec.Command("minio-client", all...)
	cmd.Env = append(os.Environ(), "MC_HOST_samogon="+s.cfg.MCHost)

	return cmd
}

// Stat returns true iff key exists. Any error (network, auth, missing)
// becomes false — callers treat "not there" as "not uploaded yet" and
// the real error surfaces on the next Put.
func (s *Storage) Stat(key string) bool {
	cmd := s.mc("stat", "--json", key)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	return cmd.Run() == nil
}

func (s *Storage) PutBytes(key string, data []byte) {
	// `mc cp -` isn't a thing; minio-client dedicates `mc pipe` to
	// stdin→S3 writes. Using cp with '-' produces the cryptic
	// "Unable to prepare URL for copying" error.
	cmd := s.mc("pipe", "--quiet", key)
	cmd.Stdin = bytes.NewReader(data)

	var e bytes.Buffer

	cmd.Stdout = os.Stderr
	cmd.Stderr = &e

	if err := cmd.Run(); err != nil {
		ThrowFmt("minio-client pipe %s: %v: %s", key, err, strings.TrimSpace(e.String()))
	}
}

func (s *Storage) PutFile(key, path string) {
	cmd := s.mc("cp", "--quiet", path, key)

	var e bytes.Buffer

	cmd.Stdout = os.Stderr
	cmd.Stderr = &e

	if err := cmd.Run(); err != nil {
		ThrowFmt("minio-client cp %s %s: %v: %s", path, key, err, strings.TrimSpace(e.String()))
	}
}

func (s *Storage) Cat(key string) []byte {
	cmd := s.mc("cat", key)

	var out, e bytes.Buffer

	cmd.Stdout = &out
	cmd.Stderr = &e

	if err := cmd.Run(); err != nil {
		ThrowFmt("minio-client cat %s: %v: %s", key, err, strings.TrimSpace(e.String()))
	}

	return out.Bytes()
}

// mcLsEntry is the subset of `mc ls --json` lines we care about.
type mcLsEntry struct {
	Status string `json:"status"`
	Key    string `json:"key"`
	Type   string `json:"type"`
}

// List returns the leaf names (last path component) under prefix. Non-
// file entries are dropped.
func (s *Storage) List(prefix string) []string {
	cmd := s.mc("ls", "--json", prefix)

	var out, e bytes.Buffer

	cmd.Stdout = &out
	cmd.Stderr = &e

	if err := cmd.Run(); err != nil {
		ThrowFmt("minio-client ls %s: %v: %s", prefix, err, strings.TrimSpace(e.String()))
	}

	var names []string

	for _, line := range strings.Split(strings.TrimRight(out.String(), "\n"), "\n") {
		if line == "" {
			continue
		}

		var e mcLsEntry

		// Skip non-JSON or malformed lines — `mc ls` occasionally
		// emits summary rows; the filter is fine here.
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			continue
		}

		if e.Status != "success" || e.Type == "folder" {
			continue
		}

		names = append(names, e.Key)
	}

	return names
}

// ListPieces returns a set of piece hashes (the leaf of each CAS key
// under PrefixPieces). Called once at fetch start so anacrolix's
// per-piece Completion() can check an in-memory map instead of
// forking minio-client per piece.
func (s *Storage) ListPieces(cfg *Config) map[string]bool {
	keys := s.List(cfg.PrefixPieces())
	out := make(map[string]bool, len(keys))

	for _, k := range keys {
		out[path.Base(k)] = true
	}

	return out
}
