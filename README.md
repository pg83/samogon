# samogon

Torrent→S3 bridge with an SFTP read-out. Two subcommands share one
binary:

- `samogon fetch` — one-shot. Reads `.torrent` bytes from stdin, joins
  the swarm via [anacrolix/torrent], and pushes each piece into MinIO
  as a content-addressable blob keyed by piece hash. The `.torrent`
  itself is uploaded last as a commit marker.
- `samogon serve` — long-lived SFTP daemon. Loads every `.torrent`
  under the root into memory, exposes each torrent as a virtual
  directory, and serves bytes on demand by fetching pieces from CAS
  (LRU in front). Read-only. Clients authenticate with
  username/password or an authorized_keys file.

Designed to run under [gorn] so the cluster schedules fetches, and
under runit so `serve` is always up on every host.

Seeding is **not** implemented.

## Architecture

```
gorn ignite <<EOF                          # script piped to ignite
#!/bin/sh
echo 'BASE64...' | base64 -d | samogon fetch
EOF
       │
       ▼
fetch: stdin .torrent → metainfo → anacrolix.Client(storage = custom)
       PieceImpl.WriteAt → buffer in RAM
       PieceImpl.MarkComplete (hash verified by anacrolix)
           → s3.PutObject torrents/pieces/<piece-hash>
       torrent.Complete → s3.PutObject .torrent
           → torrents/torrents/<infohash>

samogon serve --listen :2222 --user X --pass Y
       │
       ▼
serve: ssh.Server + SFTP subsystem
meta:  ListObjectsV2 torrents/torrents/ → GetObject each .torrent →
       parse → keep in RAM; reload every 30s
sftp:  virtual FS; Fileread.ReadAt(off, len) → piece_idx = off/piece_len
           cache.Get(hash) ?? GetObject torrents/pieces/<hash>
                           → cache.Put
```

## S3 layout

Single prefix, two kinds of objects:

```
torrents/
    torrents/<infohash>        ← .torrent blob
    pieces/<piece-hash>        ← CAS piece, exactly piece_len bytes
                                 (last piece shorter)
```

`<piece-hash>` is hex SHA-1 for v1 torrents. Pure v2 torrents are
rejected at ingest — piece hashes aren't available at the layer we key
on. Hybrid v1+v2 works (we use the v1 hash side).

## Environment

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
S3_ENDPOINT       http://host:port
S3_BUCKET         bucket name
SAMOGON_S3_ROOT   default "torrents"
```

CLI flags override env. Credentials go into the SDK's static provider;
no config file is written.

## Usage

```sh
# one-shot: join the swarm, push to CAS, commit the .torrent
samogon fetch < ubuntu-24.04.iso.torrent

# daemon: SFTP read-out
samogon serve \
    --listen :2222 \
    --user tv --pass tv \
    --host-key /etc/samogon/host_ed25519

# browse:
sftp -P 2222 tv@host
sftp> ls
ubuntu-24.04.iso
sftp> get ubuntu-24.04.iso/ubuntu.iso
```

## Idempotency

- `torrents/torrents/<infohash>` is the commit marker. If it exists,
  `fetch` exits immediately with `already-done`.
- Crash before commit? Pieces already in CAS are picked up on the next
  run via `Completion()` HEAD tests — download resumes from the first
  missing piece.
- Same piece appearing in multiple torrents dedupes naturally (CAS is
  keyed by content hash).

## Build

```sh
GOPROXY=https://proxy.golang.org,direct \
GOSUMDB=sum.golang.org \
    go build
```

Bundled under ix as `bin/samogon` (`-tags=nosqlite` to drop a crawshaw
cgo wrapper whose `cfree` collides with ix's tcmalloc).

## Style / conventions

See [`STYLE.md`](STYLE.md). `throw.go` is a byte-for-byte copy of
gorn's; error handling goes through `Throw`/`Try`. All `.go` files live
at the repo root — no `internal/`, `cmd/`, `pkg/`. S3 via
`aws-sdk-go-v2` (same SDK as gorn), one long-lived client with
connection pooling.

## What's not done

- Seeding. `PieceImpl.ReadAt` exists but isn't wired to CAS past the
  in-flight buffer.
- Magnet URIs. `.torrent` only.
- Piece prefetch on sequential reads. The LRU is reactive; for
  streaming video you may want a readahead heuristic in `sftp.go`.
- GC. Deleting a `.torrent` from S3 leaves its exclusive pieces in CAS.

[anacrolix/torrent]: https://github.com/anacrolix/torrent
[gorn]: https://github.com/pg83/gorn
