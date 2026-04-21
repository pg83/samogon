package main

import (
	"bytes"
	"crypto/subtle"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func runServe(cfg *Config) {
	store := newStorage(cfg)
	defer store.Close()

	cache := newLRU(cfg.LRUSize)
	meta := newMeta(cfg, store)

	// Initial load is advisory — the bucket may not exist yet on a
	// fresh deployment, or S3 may be transiently unreachable. Log
	// and let metaReloader pick it up on the next tick; the daemon
	// must not refuse to start just because S3 isn't ready.
	exc := Try(func() {
		meta.Reload()
	})

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, clr(clrY, "serve: initial reload failed (continuing): "+e.Error()))
	})

	go metaReloader(cfg, meta)

	sshCfg := buildSSHConfig(cfg)

	ln := Throw2(net.Listen("tcp", cfg.Listen))

	fmt.Fprintln(os.Stderr, clr(clrG, "serve: listening on "+cfg.Listen))

	for {
		conn := Throw2(ln.Accept())

		go func(c net.Conn) {
			exc := Try(func() {
				handleConn(c, sshCfg, cfg, meta, store, cache)
			})

			exc.Catch(func(e *Exception) {
				fmt.Fprintln(os.Stderr, clr(clrY, "serve: conn "+c.RemoteAddr().String()+": "+e.Error()))
			})
		}(conn)
	}
}

func metaReloader(cfg *Config, meta *Meta) {
	tick := time.NewTicker(time.Duration(cfg.ReloadSecs) * time.Second)
	defer tick.Stop()

	for range tick.C {
		exc := Try(func() {
			meta.Reload()
		})

		exc.Catch(func(e *Exception) {
			fmt.Fprintln(os.Stderr, clr(clrY, "serve: reload failed: "+e.Error()))
		})
	}
}

func buildSSHConfig(cfg *Config) *ssh.ServerConfig {
	scfg := &ssh.ServerConfig{}

	if cfg.User != "" {
		user := cfg.User
		pass := []byte(cfg.Pass)

		scfg.PasswordCallback = func(c ssh.ConnMetadata, given []byte) (*ssh.Permissions, error) {
			if c.User() != user {
				return nil, fmt.Errorf("invalid user")
			}

			if subtle.ConstantTimeCompare(given, pass) != 1 {
				return nil, fmt.Errorf("invalid password")
			}

			return nil, nil
		}
	}

	if cfg.AuthKeys != "" {
		allowed := loadAuthorizedKeys(cfg.AuthKeys)

		scfg.PublicKeyCallback = func(c ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			marshaled := key.Marshal()

			for _, k := range allowed {
				if bytes.Equal(k.Marshal(), marshaled) {
					return nil, nil
				}
			}

			return nil, fmt.Errorf("unauthorized key")
		}
	}

	signer := loadHostKey(cfg.HostKey)
	scfg.AddHostKey(signer)

	return scfg
}

func loadHostKey(path string) ssh.Signer {
	data := Throw2(os.ReadFile(path))

	return Throw2(ssh.ParsePrivateKey(data))
}

func loadAuthorizedKeys(path string) []ssh.PublicKey {
	data := Throw2(os.ReadFile(path))

	var out []ssh.PublicKey

	rest := data

	for len(bytes.TrimSpace(rest)) > 0 {
		key, _, _, next, err := ssh.ParseAuthorizedKey(rest)

		if err != nil {
			ThrowFmt("parse %s: %v", path, err)
		}

		out = append(out, key)
		rest = next
	}

	if len(out) == 0 {
		ThrowFmt("authorized_keys %s is empty", path)
	}

	return out
}

func handleConn(conn net.Conn, sshCfg *ssh.ServerConfig, cfg *Config, meta *Meta, store *Storage, cache *LRU) {
	defer conn.Close()

	_, chans, reqs, err := ssh.NewServerConn(conn, sshCfg)

	if err != nil {
		ThrowFmt("ssh handshake: %v", err)
	}

	go ssh.DiscardRequests(reqs)

	for ch := range chans {
		if ch.ChannelType() != "session" {
			_ = ch.Reject(ssh.UnknownChannelType, "only session channels supported")

			continue
		}

		go func(nc ssh.NewChannel) {
			exc := Try(func() {
				handleSession(nc, cfg, meta, store, cache)
			})

			exc.Catch(func(e *Exception) {
				fmt.Fprintln(os.Stderr, clr(clrY, "serve: session: "+e.Error()))
			})
		}(ch)
	}
}

func handleSession(nc ssh.NewChannel, cfg *Config, meta *Meta, store *Storage, cache *LRU) {
	ch, reqs, err := nc.Accept()

	if err != nil {
		ThrowFmt("accept channel: %v", err)
	}

	defer ch.Close()

	accepted := make(chan bool, 1)

	go func() {
		for req := range reqs {
			ok := false

			if req.Type == "subsystem" && isSftpPayload(req.Payload) {
				ok = true
			}

			_ = req.Reply(ok, nil)

			if ok {
				accepted <- true
			}
		}

		close(accepted)
	}()

	if !<-accepted {
		return
	}

	handlers := newSftpHandlers(cfg, meta, store, cache)
	srv := sftp.NewRequestServer(ch, handlers)
	defer srv.Close()

	_ = srv.Serve()
}

// isSftpPayload checks whether an SSH "subsystem" request payload
// names the sftp subsystem. Payload is `uint32 length || "sftp"`.
func isSftpPayload(p []byte) bool {
	if len(p) < 4 {
		return false
	}

	name := string(p[4:])

	return name == "sftp"
}
