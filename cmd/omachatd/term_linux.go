//go:build linux

package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// readSecret prompts on the terminal and reads one line without echoing it.
// It reads one byte at a time, so nothing but the secret and the line end is
// consumed, and it returns the bytes without a copy that outlives the caller:
// the caller clears the returned slice. The prompt goes to stderr.
func readSecret(prompt string) ([]byte, error) {
	fd := int(os.Stdin.Fd())
	state, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return nil, errors.New("the PIN must be typed at a terminal; run this command in a terminal window")
	}
	raw := *state
	raw.Lflag &^= unix.ECHO
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &raw); err != nil {
		return nil, fmt.Errorf("could not turn off terminal echo: %w", err)
	}
	defer func() {
		_ = unix.IoctlSetTermios(fd, unix.TCSETS, state)
		fmt.Fprintln(os.Stderr)
	}()
	fmt.Fprint(os.Stderr, prompt)
	return readLine(os.Stdin)
}

// readLine reads up to a newline from r, one byte at a time, capped at 64 bytes.
func readLine(r io.Reader) ([]byte, error) {
	line := make([]byte, 0, 64)
	one := make([]byte, 1)
	for {
		n, err := r.Read(one)
		if n == 1 {
			if one[0] == '\n' || one[0] == '\r' {
				return line, nil
			}
			if len(line) >= 64 {
				clear(line)
				return nil, errors.New("input is too long")
			}
			line = append(line, one[0])
		}
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) > 0 {
				return line, nil
			}
			clear(line)
			return nil, err
		}
	}
}
