package main

import (
	"fmt"
	"os"
	"syscall"
)

// maxHashersEnv is anacrolix's global cap on concurrent piece hashers
// (client.go: `var maxActivePieceHashers = initIntFromEnv(...)`).
// It's a package-level var initialized from the environment *once*,
// before main() runs — PieceHashersPerTorrent won't cross this floor,
// so on a 24-core box you see uploads=24 regardless of per-torrent
// config. The only lever is env-before-exec.
const (
	maxHashersEnv  = "TORRENT_MAX_ACTIVE_PIECE_HASHERS"
	maxHashersWant = "128"
)

func ensureHashersEnv() {
	if os.Getenv(maxHashersEnv) != "" {
		return
	}

	// Can't just os.Setenv here — by the time main() runs, anacrolix
	// has already read the env and cached the value. Re-exec
	// ourselves with the variable set so the child's package init
	// sees it.
	Throw(os.Setenv(maxHashersEnv, maxHashersWant))

	exe := Throw2(os.Executable())
	fmt.Fprintln(os.Stderr, clr(clrB, "samogon: re-exec with "+maxHashersEnv+"="+maxHashersWant))

	Throw(syscall.Exec(exe, os.Args, os.Environ()))
}

const usage = `usage: samogon <subcommand> [flags]

Subcommands:
  fetch       one-shot torrent downloader; reads .torrent bytes
              from stdin, writes pieces to S3 as
              torrents/pieces/<hash> and the .torrent itself as
              torrents/torrents/<infohash>
  serve       SFTP daemon streaming from S3-backed CAS
  get         local download of a single file — same Storage/
              Prefetcher/virtualFile chain as serve, no SFTP
  get2        raw-read benchmark: grabs every piece of a file in
              arbitrary order across N workers, no cache or disk —
              isolates minio/network from the get pipeline
  bench       PutObject throughput test — N goroutines spam fixed-
              size chunks at the endpoint, reports ops/s + MiB/s

Env (shared):
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
  S3_ENDPOINT, S3_BUCKET, SAMOGON_S3_ROOT

Pass --help to a subcommand for its flags.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	sub := os.Args[1]
	rest := os.Args[2:]

	if sub == "-h" || sub == "--help" || sub == "help" {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(0)
	}

	// Only fetch needs anacrolix; serve is network-only on SFTP.
	// Avoid the re-exec round trip for serve so a misconfigured env
	// doesn't mask itself across restart loops.
	if sub == "fetch" {
		ensureHashersEnv()
	}

	exc := Try(func() {
		switch sub {
		case "fetch":
			runFetch(parseFetchArgs(rest))

		case "serve":
			runServe(parseServeArgs(rest))

		case "get":
			cfg, opts := parseGetArgs(rest)
			runGet(cfg, opts)

		case "get2":
			cfg, opts := parseGet2Args(rest)
			runGet2(cfg, opts)

		case "bench":
			cfg, opts := parseBenchArgs(rest)
			runBench(cfg, opts)

		default:
			ThrowFmt("unknown subcommand: %s", sub)
		}
	})

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, clr(clrR, "abort: "+e.Error()))
		os.Exit(1)
	})
}
