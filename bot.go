package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"golang.org/x/net/proxy"
)

// bot is a Telegram bot front-end for `samogon fetch`. Users send
// .torrent files as documents; the bot downloads each file, then
// pipes the raw bytes into `gorn ignite -- samogon fetch` so the
// actual download lands on a gorn worker (same path as any scripted
// ignite). Sender allowlist is a hard requirement — without it, any
// Telegram user could schedule arbitrary fetches against the cluster.

type botOpts struct{}

func parseBotArgs(args []string) *Config {
	c := loadCommon()

	fs := flag.NewFlagSet("samogon bot", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&c.TgToken, "tg-token", os.Getenv("TG_BOT_TOKEN"), "Telegram Bot API token (env TG_BOT_TOKEN)")
	fs.StringVar(&c.TgAllowUsers, "tg-allow-users", os.Getenv("TG_ALLOW_USERS"), "comma-separated Telegram user IDs allowed to send torrents (env TG_ALLOW_USERS)")
	fs.StringVar(&c.Socks5, "socks5", os.Getenv("SAMOGON_SOCKS5"), "host:port of a SOCKS5 proxy for Telegram HTTPS calls (env SAMOGON_SOCKS5); empty = direct")

	Throw(fs.Parse(args))

	if fs.NArg() > 0 {
		ThrowFmt("samogon bot: unexpected positional args: %v", fs.Args())
	}

	if c.TgToken == "" {
		ThrowFmt("--tg-token (or TG_BOT_TOKEN) is required")
	}

	if c.TgAllowUsers == "" {
		ThrowFmt("--tg-allow-users (or TG_ALLOW_USERS) is required (empty = deny all, makes no sense)")
	}

	return c
}

func runBot(cfg *Config) {
	allow := parseAllowUsers(cfg.TgAllowUsers)

	if len(allow) == 0 {
		ThrowFmt("bot: allow list is empty after parsing %q", cfg.TgAllowUsers)
	}

	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf("bot: allowing %d user(s)", len(allow))))

	// Lab hosts don't have direct outbound to api.telegram.org; route
	// both the Bot API client and file downloads through the cluster
	// SOCKS5 proxy when --socks5 (or SAMOGON_SOCKS5) is set. One
	// shared http.Client keeps TLS sessions + keepalives alive across
	// both paths.
	httpClient := http.DefaultClient

	if cfg.Socks5 != "" {
		fmt.Fprintln(os.Stderr, clr(clrB, "bot: routing HTTPS via socks5 "+cfg.Socks5))
		httpClient = newSocks5Client(cfg.Socks5)
	}

	api := Throw2(tgbotapi.NewBotAPIWithClient(cfg.TgToken, tgbotapi.APIEndpoint, httpClient))
	fmt.Fprintln(os.Stderr, clr(clrG, "bot: authorized as @"+api.Self.UserName))

	upd := tgbotapi.NewUpdate(0)
	// Stay shorter than the narrowest idle timeout in the
	// bot → haproxy(8015) → ssh -D → exit → api.telegram.org chain.
	// Observed EOF at ~54s in production → 30s leaves plenty of
	// headroom without noticeably extra API traffic.
	upd.Timeout = 30
	upd.AllowedUpdates = []string{"message"}

	updates := api.GetUpdatesChan(upd)

	for u := range updates {
		msg := u.Message

		if msg == nil || msg.From == nil {
			continue
		}

		if !allow[msg.From.ID] {
			fmt.Fprintln(os.Stderr, clr(clrY, fmt.Sprintf(
				"bot: rejected from %d (@%s): not in allow list",
				msg.From.ID, msg.From.UserName)))

			reply(api, msg.Chat.ID, msg.MessageID, "not authorized")

			continue
		}

		if msg.Document == nil {
			reply(api, msg.Chat.ID, msg.MessageID, "send a .torrent file as a document")

			continue
		}

		go handleDocument(api, cfg, msg)
	}
}

func parseAllowUsers(csv string) map[int64]bool {
	out := map[int64]bool{}

	for _, s := range strings.Split(csv, ",") {
		s = strings.TrimSpace(s)

		if s == "" {
			continue
		}

		id, err := strconv.ParseInt(s, 10, 64)

		if err != nil {
			ThrowFmt("bot: bad user ID %q in allow list: %v", s, err)
		}

		out[id] = true
	}

	return out
}

func handleDocument(api *tgbotapi.BotAPI, cfg *Config, msg *tgbotapi.Message) {
	doc := msg.Document

	fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf(
		"bot: received %q (%d bytes) from %d (@%s)",
		doc.FileName, doc.FileSize, msg.From.ID, msg.From.UserName)))

	// Telegram file download: GetFile returns a path; Link() composes
	// the full https URL using the bot token.
	file, err := api.GetFile(tgbotapi.FileConfig{FileID: doc.FileID})

	if err != nil {
		reply(api, msg.Chat.ID, msg.MessageID, "telegram getFile failed: "+err.Error())

		return
	}

	// Reuse the bot API's client so file downloads go through the
	// same SOCKS5 proxy (if configured) + TLS session cache.
	resp, err := api.Client.(*http.Client).Get(file.Link(cfg.TgToken))

	if err != nil {
		reply(api, msg.Chat.ID, msg.MessageID, "download failed: "+err.Error())

		return
	}

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)

	if err != nil {
		reply(api, msg.Chat.ID, msg.MessageID, "read failed: "+err.Error())

		return
	}

	// Spawn `gorn ignite ... -- samogon fetch`, pipe torrent bytes
	// on stdin — ignite's synthesizeScript embeds them as base64
	// and the worker pipes them into samogon fetch's stdin.
	//
	// No --wait: fire-and-forget. ignite POSTs the task to the gorn
	// control API and returns immediately with the GUID on stdout.
	// The bot replies with that GUID so the user can track progress
	// via gorn web / `gorn ignite --wait --guid <G>` later.
	//
	//  --api      inherited from $GORN_API in our env via ignite's
	//             own fallback (ignite inherits env by default).
	//  --root     S3 prefix for ignite's own artifacts (stdout,
	//             stderr, result.json); keeps them under
	//             gorn/samogon/ instead of mixing with CI's gorn/cli/.
	//  --env K=V  forward every env key samogon fetch actually needs
	//             into the worker side. ignite does not propagate
	//             the bot's ambient env; only what we pass here
	//             lands on the worker.
	//  --descr    human-readable task description — shown in gorn web.
	args := []string{
		"ignite",
		"--root", "samogon",
		"--descr", "samogon fetch " + doc.FileName,
		// samogon fetch reports torrent outcome through S3 side-channels;
		// any exit != 0 we see from it is infra noise (gorn worker OOM,
		// S3 hiccup) that should bounce back onto the queue instead of
		// silently getting discarded as a "non-retriable" build failure.
		"--retry-error",
	}

	for _, k := range []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"S3_ENDPOINT",
		"S3_BUCKET",
		"SAMOGON_S3_ROOT",
	} {
		if v := os.Getenv(k); v != "" {
			args = append(args, "--env", k+"="+v)
		}
	}

	args = append(args, "--", "samogon", "fetch")

	var out bytes.Buffer

	cmd := exec.Command("gorn", args...)
	cmd.Stdin = bytes.NewReader(raw)
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		reply(api, msg.Chat.ID, msg.MessageID, "ignite failed: "+err.Error())

		return
	}

	guid := strings.TrimSpace(out.String())
	reply(api, msg.Chat.ID, msg.MessageID, fmt.Sprintf("queued: %s  guid=%s", doc.FileName, guid))
}

func reply(api *tgbotapi.BotAPI, chatID int64, replyTo int, text string) {
	m := tgbotapi.NewMessage(chatID, text)
	m.ReplyToMessageID = replyTo

	if _, err := api.Send(m); err != nil {
		fmt.Fprintln(os.Stderr, clr(clrY, "bot: reply failed: "+err.Error()))
	}
}

// newSocks5Client wires a SOCKS5 dialer into an http.Transport so
// every request issued by the returned client hops through the
// proxy. golang.org/x/net/proxy provides the context-aware dialer
// that http.Transport.DialContext expects.
func newSocks5Client(addr string) *http.Client {
	d, err := proxy.SOCKS5("tcp", addr, nil, proxy.Direct)

	if err != nil {
		ThrowFmt("bot: socks5 dialer %q: %v", addr, err)
	}

	cd, ok := d.(proxy.ContextDialer)

	if !ok {
		ThrowFmt("bot: socks5 dialer %q is not a ContextDialer", addr)
	}

	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				return cd.DialContext(ctx, network, address)
			},
		},
	}
}
