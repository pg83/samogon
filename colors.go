package main

import "os"

const (
	clrRst = "\x1b[0m"
	clrR   = "\x1b[91m"
	clrG   = "\x1b[92m"
	clrY   = "\x1b[93m"
	clrB   = "\x1b[94m"
)

var useColor = func() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}

	if os.Getenv("TERM") == "dumb" {
		return false
	}

	fi, err := os.Stderr.Stat()

	if err != nil {
		return false
	}

	return (fi.Mode() & os.ModeCharDevice) != 0
}()

func clr(code, text string) string {
	if !useColor {
		return text
	}

	return code + text + clrRst
}
