package main

import (
	"fmt"
	"os"
)

const usage = `usage: samogon <subcommand> [flags]

Subcommands:
  fetch       one-shot torrent downloader; reads .torrent bytes
              from stdin, writes pieces to S3 as
              torrents/pieces/<hash> and the .torrent itself as
              torrents/torrents/<infohash>
  serve       SFTP daemon streaming from S3-backed CAS

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

	exc := Try(func() {
		switch sub {
		case "fetch":
			runFetch(parseFetchArgs(rest))

		case "serve":
			runServe(parseServeArgs(rest))

		default:
			ThrowFmt("unknown subcommand: %s", sub)
		}
	})

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, clr(clrR, "abort: "+e.Error()))
		os.Exit(1)
	})
}
