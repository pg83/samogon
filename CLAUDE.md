# samogon — context for Claude

Torrent→S3 bridge. Two subcommands share one binary:

- `samogon fetch <base64>` — one-shot. Decodes a .torrent, joins the swarm, writes each piece into MinIO as `torrents/pieces/<piece-hash>` (content-addressable). On completion writes the .torrent itself to `torrents/torrents/<infohash>`. Designed to run under `gorn ignite -- samogon fetch ...` so the cluster schedules downloads.
- `samogon serve` — long-lived SFTP daemon. Loads every `.torrent` under `torrents/torrents/` into memory, exposes each as a virtual directory, serves piece bytes on demand by fetching `torrents/pieces/<hash>` through a small in-memory LRU.

Seeding is **not** implemented. The anacrolix storage `ReadAt` path is a stub.

## Coding conventions

- Git author: `claude <claude@users.noreply.github.com>`. Commit messages in English.

## Architecture

```
gorn ignite -- samogon fetch <b64>
       │
       ▼
fetch.go:
  base64 → metainfo → anacrolix.Client(storage=samogonStorage)
  PieceImpl.WriteAt → buffer in RAM
  PieceImpl.MarkComplete (hash already verified by anacrolix) →
      s3.PutObject torrents/pieces/<hash>
  on torrent.Complete → s3.PutObject torrents/torrents/<infohash>

samogon serve --listen :2222 --user X --pass Y
       │
       ▼
serve.go: ssh.Server + SFTP subsystem
meta.go: ListObjectsV2 torrents/torrents/ → GetObject each .torrent →
         parse → keep in RAM; reload every 30s
sftp.go: Handlers{Fileread,Filelist}
         Fileread ReadAt(off, len) → piece_index = off/piece_len →
             cache.Get(hash) ?? GetObject torrents/pieces/<hash> → cache.Put
cache.go: LRU(1000) keyed by piece-hash
```

## S3 layout

Single prefix, two kinds:

```
torrents/
    torrents/<infohash>          ← .torrent blob
    pieces/<piece-hash>          ← CAS piece, bytes exactly piece_len (last piece shorter)
```

`<piece-hash>` is hex SHA-1 for v1 torrents, SHA-256 for v2. anacrolix gives us the hash via `metainfo.Piece.Hash()`.

## Non-negotiable rules

Inherits wholesale from `gorn/STYLE.md`:

- **Error handling goes through `Throw` / `Try`** (`throw.go`, byte-for-byte copy of gorn's). No `if err != nil { return err }` pass-through. Catches at boundaries: `main`, goroutine entries, SFTP handler entries (otherwise a panic kills the SSH connection or the whole daemon).
- **Blank lines around `if`/`for`/`switch`/`select`/`go`/`defer` and before `return`**, unless first/last inside `{}`.
- **Flat layout.** All `.go` files at repo root. No `internal/`, `cmd/`, `pkg/`.
- **Config is JSON if there's a config file, never YAML.** Currently only flags + env; no config file yet.
- **Never truncate output.** No `...(truncated)`, no `head -c`.

## Dependencies

- `github.com/anacrolix/torrent` — BitTorrent client. We plug in a custom `storage.ClientImpl`; anacrolix handles peers/trackers/DHT/piece verification.
- `github.com/pkg/sftp` — SFTP server handlers.
- `golang.org/x/crypto/ssh` — SSH server (transport for SFTP).
- **S3 via `aws-sdk-go-v2`.** Matches gorn. Earlier revisions shelled out to `minio-client` per piece — at 200-300ms of fork+exec+TCP overhead each, upload throughput capped at ~5-10 pieces/s regardless of anacrolix's hasher parallelism. The SDK keeps one long-lived HTTP client with connection pooling; PutObject calls happen in parallel over reused TCP connections and hit the rate the underlying minio can actually absorb.

## Env required at runtime

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
S3_ENDPOINT            http(s)://host:port
S3_BUCKET              bucket name
SAMOGON_S3_ROOT        (default "torrents")
```

CLI flags override env. Credentials go straight into a `credentials.NewStaticCredentialsProvider`; `S3_ENDPOINT` is passed as `BaseEndpoint` to the S3 client with `UsePathStyle=true` (MinIO requires it).

## Invariants

- **infohash is the only key.** `torrents/torrents/<infohash>` always exists iff the torrent's pieces are uploaded. `fetch` uploads the .torrent **last**, after every piece is in CAS; if `fetch` crashes mid-download, the next run picks up pieces from CAS via `Completion()` HEAD checks and resumes.
- **Piece hashes are trusted.** We do not re-verify on read — anacrolix verified before `MarkComplete`, so the CAS content is known-good. Reader in `serve` treats the blob as opaque.
- **SFTP is read-only.** `Filewrite`/`Filecmd` reject everything; nothing ever writes back to MinIO through `serve`.
- **No seeding.** The fetch client leaves the swarm as soon as the torrent completes. anacrolix's `PieceImpl.ReadAt` is not wired to CAS — if we ever flip on seeding, that's the hook to implement.

## Build / run

```
go build        # produces ./samogon

# fetch (one-shot)
./samogon fetch "$(base64 < ubuntu.torrent)"

# serve (daemon)
./samogon serve --listen :2222 --user tv --pass tv --host-key /etc/samogon/host_ed25519
```

## What's not done

- Seeding.
- Magnet URIs. .torrent only; magnets need DHT metadata fetch first. Flip later by letting fetch.go accept a magnet URI and using anacrolix's `AddMagnet`.
- Piece prefetch on sequential reads. Current LRU is reactive. If streaming video is the real workload, add a readahead heuristic in `sftp.go` (detect two consecutive reads, kick off next N piece fetches).
- Auth beyond `--user/--pass` + `--authorized-keys`. No LDAP, no certs.
- GC. Deleting a torrent doesn't remove its exclusive pieces — they stay in CAS. Add `samogon gc` later (scan all .torrent → union of piece hashes → delete any `torrents/pieces/` not in the set).

## Misc

- `fetch` runs one torrent per process. anacrolix is fine with multi-torrent clients, but gorn schedules one job at a time on a given endpoint and that's where the parallelism lives.
- minio uploads from `fetch` are capped by a semaphore (currently 4) so a fast-completing torrent doesn't hammer the endpoint with concurrent `cp`s.
- SFTP root lists torrents by **name from the .torrent**, not infohash — so `ls` is human-readable. Same-name torrents with different infohashes disambiguate via `-<infohash[:8]>` suffix.
