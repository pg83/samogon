package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"
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

// isTransientMCError decides whether a minio-client stderr dump looks
// like a transient condition worth retrying (network hiccups, server
// restarts, rate-limits) vs a steady-state failure (auth, missing
// bucket, malformed request).
func isTransientMCError(stderr string) bool {
	needles := []string{
		"no such host",
		"connection refused",
		"connection reset",
		"connection timed out",
		"i/o timeout",
		"broken pipe",
		"EOF",
		"Unable to connect",
		"temporarily unavailable",
		"RequestTimeout",
		"SlowDown",
		"InternalError",
		"ServiceUnavailable",
		"503 Service Unavailable",
		"502 Bad Gateway",
		"504 Gateway Timeout",
	}

	for _, n := range needles {
		if strings.Contains(stderr, n) {
			return true
		}
	}

	return false
}

// isNotFoundMCError matches the S3/MinIO "this thing doesn't exist"
// family. Used by List callers to turn "no such bucket / no such key"
// into an empty listing (legitimately the same thing as "empty") while
// still propagating auth / network errors.
func isNotFoundMCError(stderr string) bool {
	needles := []string{
		"specified bucket does not exist",
		"specified key does not exist",
		"Object does not exist",
		"NoSuchBucket",
		"NoSuchKey",
	}

	for _, n := range needles {
		if strings.Contains(stderr, n) {
			return true
		}
	}

	return false
}

// withRetry runs fn with exponential backoff on transient errors. On a
// non-transient error it throws immediately (retrying an auth failure
// just spams the log). On exhausted retries it throws with the last
// stderr attached so the caller sees what kept failing.
func (s *Storage) withRetry(label string, fn func() (string, error)) {
	const maxAttempts = 6

	backoff := 500 * time.Millisecond

	var lastErr error
	var lastStderr string

	for attempt := 0; attempt < maxAttempts; attempt++ {
		stderr, err := fn()

		if err == nil {
			return
		}

		lastErr = err
		lastStderr = stderr

		if !isTransientMCError(stderr) {
			ThrowFmt("minio-client %s: %v: %s", label, err, stderr)
		}

		if attempt == maxAttempts-1 {
			break
		}

		fmt.Fprintln(os.Stderr, clr(clrY, fmt.Sprintf(
			"minio-client %s: transient error (%s); retry %d/%d in %v",
			label, stderr, attempt+1, maxAttempts, backoff)))

		time.Sleep(backoff)
		backoff *= 2

		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}

	ThrowFmt("minio-client %s: gave up after %d attempts: %v: %s",
		label, maxAttempts, lastErr, lastStderr)
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
	s.withRetry("pipe "+key, func() (string, error) {
		cmd := s.mc("pipe", "--quiet", key)
		cmd.Stdin = bytes.NewReader(data)

		var e bytes.Buffer

		cmd.Stdout = os.Stderr
		cmd.Stderr = &e

		err := cmd.Run()

		return strings.TrimSpace(e.String()), err
	})
}

func (s *Storage) PutFile(key, path string) {
	s.withRetry("cp "+path+" "+key, func() (string, error) {
		cmd := s.mc("cp", "--quiet", path, key)

		var e bytes.Buffer

		cmd.Stdout = os.Stderr
		cmd.Stderr = &e

		err := cmd.Run()

		return strings.TrimSpace(e.String()), err
	})
}

func (s *Storage) Cat(key string) []byte {
	var out bytes.Buffer

	s.withRetry("cat "+key, func() (string, error) {
		out.Reset()

		cmd := s.mc("cat", key)

		var e bytes.Buffer

		cmd.Stdout = &out
		cmd.Stderr = &e

		err := cmd.Run()

		return strings.TrimSpace(e.String()), err
	})

	return out.Bytes()
}

// mcLsEntry is the subset of `mc ls --json` lines we care about.
type mcLsEntry struct {
	Status string `json:"status"`
	Key    string `json:"key"`
	Type   string `json:"type"`
}

// List returns the leaf names (last path component) under prefix. Non-
// file entries are dropped. A "bucket/key does not exist" error is
// treated as an empty listing (same observable state) — auth and
// network failures still propagate through withRetry.
func (s *Storage) List(prefix string) []string {
	var out bytes.Buffer

	exc := Try(func() {
		s.withRetry("ls "+prefix, func() (string, error) {
			out.Reset()

			cmd := s.mc("ls", "--json", prefix)

			var e bytes.Buffer

			cmd.Stdout = &out
			cmd.Stderr = &e

			err := cmd.Run()

			return strings.TrimSpace(e.String()), err
		})
	})

	if exc != nil {
		if isNotFoundMCError(exc.Error()) {
			return nil
		}

		exc.throw()
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
