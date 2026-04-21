package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	AWSKey    string
	AWSSecret string
	S3Endpt   string
	S3Bucket  string
	S3Root    string

	Listen     string
	User       string
	Pass       string
	AuthKeys   string
	HostKey    string
	ReloadSecs int
	LRUSize    int
	UpSem      int
	Region     string

	DataDir string
}

func loadCommon() *Config {
	c := &Config{
		S3Root:     "torrents",
		ReloadSecs: 30,
		LRUSize:    1000,
		UpSem:      128,
		Region:     "us-east-1",
	}

	if v := os.Getenv("AWS_ACCESS_KEY_ID"); v != "" {
		c.AWSKey = v
	}

	if v := os.Getenv("AWS_SECRET_ACCESS_KEY"); v != "" {
		c.AWSSecret = v
	}

	if v := os.Getenv("S3_ENDPOINT"); v != "" {
		c.S3Endpt = v
	}

	if v := os.Getenv("S3_BUCKET"); v != "" {
		c.S3Bucket = v
	}

	if v := os.Getenv("SAMOGON_S3_ROOT"); v != "" {
		c.S3Root = v
	}

	return c
}

func parseFetchArgs(args []string) *Config {
	c := loadCommon()

	fs := flag.NewFlagSet("samogon fetch", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&c.AWSKey, "aws-key", c.AWSKey, "S3 access key (env AWS_ACCESS_KEY_ID)")
	fs.StringVar(&c.AWSSecret, "aws-secret", c.AWSSecret, "S3 secret key (env AWS_SECRET_ACCESS_KEY)")
	fs.StringVar(&c.S3Endpt, "endpoint", c.S3Endpt, "S3 endpoint URL (env S3_ENDPOINT)")
	fs.StringVar(&c.S3Bucket, "bucket", c.S3Bucket, "S3 bucket (env S3_BUCKET)")
	fs.StringVar(&c.S3Root, "s3-root", c.S3Root, "S3 key prefix (env SAMOGON_S3_ROOT)")
	fs.StringVar(&c.Region, "region", c.Region, "S3 region (MinIO ignores it)")
	fs.IntVar(&c.UpSem, "up-parallel", c.UpSem, "max concurrent PutObject calls")
	fs.StringVar(&c.DataDir, "data-dir", "", "anacrolix scratch dir (default: mkdtemp under cwd)")

	Throw(fs.Parse(args))

	if fs.NArg() > 0 {
		ThrowFmt("samogon fetch: unexpected positional args: %v (.torrent comes on stdin)", fs.Args())
	}

	validate(c)

	return c
}

func parseServeArgs(args []string) *Config {
	c := loadCommon()

	fs := flag.NewFlagSet("samogon serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&c.AWSKey, "aws-key", c.AWSKey, "S3 access key (env AWS_ACCESS_KEY_ID)")
	fs.StringVar(&c.AWSSecret, "aws-secret", c.AWSSecret, "S3 secret key (env AWS_SECRET_ACCESS_KEY)")
	fs.StringVar(&c.S3Endpt, "endpoint", c.S3Endpt, "S3 endpoint URL (env S3_ENDPOINT)")
	fs.StringVar(&c.S3Bucket, "bucket", c.S3Bucket, "S3 bucket (env S3_BUCKET)")
	fs.StringVar(&c.S3Root, "s3-root", c.S3Root, "S3 key prefix (env SAMOGON_S3_ROOT)")
	fs.StringVar(&c.Region, "region", c.Region, "S3 region (MinIO ignores it)")
	fs.StringVar(&c.Listen, "listen", ":2222", "SFTP listen address")
	fs.StringVar(&c.User, "user", "", "SFTP username for password auth (empty = no password auth)")
	fs.StringVar(&c.Pass, "pass", "", "SFTP password for password auth")
	fs.StringVar(&c.AuthKeys, "authorized-keys", "", "path to authorized_keys file (empty = no pubkey auth)")
	fs.StringVar(&c.HostKey, "host-key", "", "path to SSH host private key (PEM)")
	fs.IntVar(&c.ReloadSecs, "reload", c.ReloadSecs, "torrents/ reload interval (seconds)")
	fs.IntVar(&c.LRUSize, "lru", c.LRUSize, "piece cache size (entries)")

	Throw(fs.Parse(args))

	if fs.NArg() > 0 {
		ThrowFmt("samogon serve: unexpected positional args: %v", fs.Args())
	}

	if c.HostKey == "" {
		ThrowFmt("--host-key is required")
	}

	if c.User == "" && c.AuthKeys == "" {
		ThrowFmt("at least one of --user/--pass or --authorized-keys is required")
	}

	if c.User != "" && c.Pass == "" {
		ThrowFmt("--user set without --pass")
	}

	validate(c)

	return c
}

func validate(c *Config) {
	if c.AWSKey == "" {
		ThrowFmt("AWS_ACCESS_KEY_ID / --aws-key is required")
	}

	if c.AWSSecret == "" {
		ThrowFmt("AWS_SECRET_ACCESS_KEY / --aws-secret is required")
	}

	if c.S3Endpt == "" {
		ThrowFmt("S3_ENDPOINT / --endpoint is required")
	}

	if c.S3Bucket == "" {
		ThrowFmt("S3_BUCKET / --bucket is required")
	}

	if !strings.Contains(c.S3Endpt, "://") {
		ThrowFmt("S3_ENDPOINT missing scheme (expected http://... or https://...): %q", c.S3Endpt)
	}
}

// S3 key helpers — single source of truth for both fetch and serve, so
// the layout can't drift between the writer and the reader. Keys are
// rooted at the bucket (no alias prefix), since we talk to S3 directly
// via the SDK rather than routing through minio-client aliases.

func (c *Config) KeyTorrent(infohash string) string {
	return fmt.Sprintf("%s/torrents/%s", c.S3Root, infohash)
}

func (c *Config) KeyPiece(hash string) string {
	return fmt.Sprintf("%s/pieces/%s", c.S3Root, hash)
}

func (c *Config) PrefixTorrents() string {
	return fmt.Sprintf("%s/torrents/", c.S3Root)
}

func (c *Config) PrefixPieces() string {
	return fmt.Sprintf("%s/pieces/", c.S3Root)
}
