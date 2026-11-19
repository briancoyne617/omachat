package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/onelegdave/omachat/internal/store"
	"github.com/onelegdave/omachat/internal/wire"
)

const messengerRestoreUsage = `Usage: omachatd messenger-restore

Restores older end-to-end encrypted Messenger history with your Messenger
secure-storage PIN (the six-digit PIN from Messenger > Settings > Privacy &
safety > End-to-end encrypted chats > Message storage).

The PIN is read from the terminal without echo, sent only to Meta over the
running omachatd, and is never stored. It is tried once: Meta counts wrong
PINs and locks the backup after too many, so this command never retries.
omachatd must be running with Messenger paired and connected. The same
restore is available in OmaChat > Settings > Messenger history.
`

// restoreFrame builds the request line by hand so the PIN goes from the
// caller's slice into one buffer the caller can clear, with no intermediate
// string or encoder copy. Like pair, the request must identify itself as a
// safe client or the daemon refuses it; see wire.SafeClient. The PIN is
// digits only (checked by the caller), so it needs no JSON escaping.
func restoreFrame(pin []byte) []byte {
	frame := []byte(`{"id":"restore","network":"` + wire.NetworkMessenger +
		`","method":"` + wire.MethodMessengerRestoreHistory +
		`","client":"` + wire.SafeClient + `","params":{"pin":"`)
	frame = append(frame, pin...)
	return append(frame, `"}}`+"\n"...)
}

func runMessengerRestore(args []string) error {
	if len(args) > 0 {
		fmt.Print(messengerRestoreUsage)
		if args[0] == "-h" || args[0] == "--help" {
			return nil
		}
		return errors.New("unrecognized arguments")
	}
	pin, err := readSecret("Messenger secure-storage PIN (6 digits): ")
	if err != nil {
		return err
	}
	defer clear(pin)
	if len(pin) != 6 || !allDigits(pin) {
		return errors.New("the PIN is six digits")
	}
	paths, err := store.NewPaths()
	if err != nil {
		return err
	}
	conn, err := net.Dial("unix", paths.SocketPath())
	if err != nil {
		return fmt.Errorf("connect to omachatd (is it running?): %w", err)
	}
	defer conn.Close()
	return restoreOver(conn, pin, os.Stderr)
}

func allDigits(b []byte) bool {
	for _, c := range b {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// restoreOver sends one restore request and follows the Messenger status
// events until the restore finishes either way. It sends the PIN once.
func restoreOver(conn io.ReadWriter, pin []byte, out io.Writer) error {
	if len(pin) != 6 || !allDigits(pin) {
		return errors.New("the PIN is six digits")
	}
	line := restoreFrame(pin)
	_, err := conn.Write(line)
	clear(line)
	if err != nil {
		return fmt.Errorf("send restore request: %w", err)
	}
	clear(pin)
	if c, ok := conn.(net.Conn); ok {
		_ = c.SetReadDeadline(time.Now().Add(45 * time.Minute))
	}
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 8192), 1<<20)
	last := ""
	report := func(h wire.HistoryRestore) {
		text := h.Text
		if h.Total > 0 && h.State == wire.HistoryRestoring {
			text = fmt.Sprintf("%s (%d/%d)", text, h.Done, h.Total)
		}
		if text != "" && text != last {
			fmt.Fprintln(out, text)
			last = text
		}
	}
	for scanner.Scan() {
		var frame struct {
			ID      string          `json:"id"`
			OK      bool            `json:"ok"`
			Error   string          `json:"error"`
			Event   string          `json:"event"`
			Network string          `json:"network"`
			Result  json.RawMessage `json:"result"`
			Data    json.RawMessage `json:"data"`
		}
		if json.Unmarshal(scanner.Bytes(), &frame) != nil {
			continue
		}
		var h wire.HistoryRestore
		switch {
		case frame.ID == "restore":
			if !frame.OK {
				return errors.New(frame.Error)
			}
			if json.Unmarshal(frame.Result, &h) != nil {
				return errors.New("unexpected reply from omachatd")
			}
		case frame.Event == wire.EventStatus && frame.Network == wire.NetworkMessenger:
			var status wire.Status
			if json.Unmarshal(frame.Data, &status) != nil || status.HistoryRestore == nil {
				continue
			}
			h = *status.HistoryRestore
		default:
			continue
		}
		report(h)
		if done, err := restoreOutcome(h); done {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("omachatd closed the connection before the restore finished")
}

// restoreOutcome reports whether a state ends the command, and with what
// error. A wrong PIN is an error and is never retried here.
func restoreOutcome(h wire.HistoryRestore) (bool, error) {
	switch h.State {
	case wire.HistoryRestored:
		return true, nil
	case wire.HistoryNoBackup:
		return true, errors.New(h.Text)
	case wire.HistoryWrongPIN, wire.HistoryPINLocked, wire.HistoryFailed, wire.HistoryUnconfirmed:
		return true, errors.New(h.Text)
	}
	return false, nil
}
