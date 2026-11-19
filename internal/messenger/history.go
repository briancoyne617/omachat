package messenger

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	appStore "github.com/onelegdave/omachat/internal/store"
	"github.com/onelegdave/omachat/internal/wire"
	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	metaTypes "go.mau.fi/mautrix-meta/pkg/messagix/types"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
)

// Encrypted Messenger history restore, ported from mautrix-meta v0.2611.0
// (pkg/connector/login_history.go, history.go and threadbackfill.go) onto
// OmaChat's own Messenger backend. The cryptography and network calls live in
// the vendored securehistory package; this file holds the orchestration:
// PIN recovery and device enrollment, the persisted device and per-chat
// cursors, and paging an encrypted chat's archive on demand.

const (
	historyFileVersion = 1
	// historyPageSize is how many messages one archive request asks for.
	historyPageSize = 50
	// historyMaxPagesPerRequest bounds the archive pages one "load older"
	// request may fetch when pages hold nothing displayable.
	historyMaxPagesPerRequest = 5
	// historyThreadPause spaces per-chat requests during a restore.
	historyThreadPause = 300 * time.Millisecond
	// e2eeSyncGroup is the Lightspeed sync group holding encrypted threads.
	e2eeSyncGroup = 95
	// historyThreadListBatches bounds E2EE thread-list pagination.
	historyThreadListBatches = 100
	historyThreadListWait    = 45 * time.Second
	historyUploadTimeout     = 2 * time.Minute
)

var historyPINPattern = regexp.MustCompile(`^[0-9]{6}$`)

// Fixed sentences shown to the user. Provider error text is never forwarded.
const (
	textHistoryPINFormat   = "Enter your six-digit Messenger PIN."
	textHistoryNotReady    = "Messenger's encrypted connection is not ready yet. Try again in a moment."
	textHistoryNoBackup    = "This Messenger account has no encrypted chat backup, so there is no older history to restore."
	textHistoryChecking    = "Checking your Messenger backup..."
	textHistoryVerifying   = "Verifying your PIN with Messenger..."
	textHistoryEnrolling   = "Setting up encrypted history on this desktop..."
	textHistoryLocked      = "Messenger reports no PIN attempts remaining. Reset your PIN in the Messenger app (Settings > Privacy & safety > End-to-end encrypted chats > Message storage), then try again."
	textHistoryPINFailed   = "Messenger could not verify the PIN right now. Nothing was changed. Wait a few minutes before trying again, because each wrong attempt counts against your PIN."
	textHistorySession     = "Messenger signed this session out. Pair Messenger again, then retry."
	textHistoryNetwork     = "Could not reach Messenger's backup service. Check your connection and try again."
	textHistoryReadFailed  = "Could not read Messenger's backup. Nothing was changed. Try again later; if it keeps failing, Meta may have changed the backup format."
	textHistoryRejected    = "Messenger did not accept this desktop as a backup device. Nothing was restored. Try again later."
	textHistoryUnconfirmed = "Messenger did not confirm the backup setup. To start over, unpair and pair Messenger again."
	textHistoryInterrupted = "Restoring history was interrupted. Choose Restore again to continue; no PIN is needed."
	textHistoryBusy        = "A restore is already running."
	textHistoryRestored    = "Encrypted history is restored on this desktop. Older messages load as you scroll."
	textHistoryAvailable   = "This account has an encrypted chat backup. Enter your Messenger PIN to restore older messages."
)

// threadHistory is the restore cursor for one encrypted chat.
type threadHistory struct {
	// Fetched is set after the first archive page was read.
	Fetched bool `json:"fetched,omitempty"`
	// Before is the oldest archive position read so far, in milliseconds.
	// Everything newer than it is already stored locally.
	Before int64 `json:"before,omitempty"`
	// ArchiveDone is set when the archive reports no more older messages.
	ArchiveDone bool `json:"archiveDone,omitempty"`
	// OpenDone is set when the pre-encryption thread has no more history.
	OpenDone bool `json:"openDone,omitempty"`
}

// cutoverLink joins an encrypted chat to the unencrypted thread it replaced.
type cutoverLink struct {
	OpenThread  int64 `json:"open"`
	ShowHistory bool  `json:"show"`
}

// historyFile is the private on-disk restore state. Device holds the backup
// device's private keys and the epoch root keys, exactly what mautrix-meta
// keeps in its login metadata. The PIN and the recovery code are never here.
type historyFile struct {
	Version        int                       `json:"version"`
	Device         *securehistory.Device     `json:"device,omitempty"`
	Threads        map[string]*threadHistory `json:"threads,omitempty"`
	Cutovers       map[string]cutoverLink    `json:"cutovers,omitempty"`
	ThreadListDone bool                      `json:"threadListDone,omitempty"`
}

// historyService is the part of the secure-history client this backend uses.
// The seam lets tests replace every call that reaches Meta.
type historyService interface {
	GetBackup(ctx context.Context) (*securehistory.Backup, error)
	RecoverPIN(ctx context.Context, backup *securehistory.Backup, pin []byte) (*securehistory.Recovery, error)
	Enroll(ctx context.Context, backup *securehistory.Backup, userID int64, code []byte, identity *keys.KeyPair) (*securehistory.Enrollment, error)
	AddDevice(ctx context.Context, enrollment *securehistory.Enrollment) (*securehistory.Device, error)
	FetchMessages(ctx context.Context, device *securehistory.Device, threadID string, before bool, timestampMS int64, count int) (*securehistory.MessageRange, error)
	Decrypt(ctx context.Context, page *securehistory.MessageRange, device *securehistory.Device, threadID string, selfID int64, isGroup bool) []*securehistory.DecryptedMessage
	UploadSentMessage(ctx context.Context, msg *securehistory.MinosSentMessage) error
	CanUpload() bool
}

// secureHistoryClient adapts the vendored client to historyService.
type secureHistoryClient struct{ *securehistory.Client }

func (c secureHistoryClient) Enroll(ctx context.Context, backup *securehistory.Backup, userID int64, code []byte, identity *keys.KeyPair) (*securehistory.Enrollment, error) {
	virtual, err := c.GetVirtualDevice(ctx, backup, userID, code)
	if err != nil {
		return nil, err
	}
	epochs, err := virtual.DeriveEpochs()
	if err != nil {
		return nil, err
	}
	return virtual.PrepareDevice(backup.ID, c.RegistrationID, identity, epochs)
}

func (c secureHistoryClient) Decrypt(ctx context.Context, page *securehistory.MessageRange, device *securehistory.Device, threadID string, selfID int64, isGroup bool) []*securehistory.DecryptedMessage {
	return page.Decrypt(ctx, device, threadID, selfID, isGroup)
}

func (c secureHistoryClient) CanUpload() bool { return c.AppID != "" }

func defaultHistoryService(cli *messagix.Client) historyService {
	return secureHistoryClient{cli.GetSecureHistoryClient()}
}

// historyClient returns the secure-history client, or nil before the Messenger
// session is loaded.
func (b *Backend) historyClient() historyService {
	b.mu.RLock()
	cli, factory := b.client, b.newHistory
	b.mu.RUnlock()
	if factory != nil {
		return factory(cli)
	}
	if cli == nil {
		return nil
	}
	return defaultHistoryService(cli)
}

// loadHistoryLocked reads the persisted state once. A missing or unreadable
// file means no restore has happened. Caller holds b.histMu.
func (b *Backend) loadHistoryLocked() {
	if b.histLoaded {
		return
	}
	b.histLoaded = true
	b.hist = historyFile{}
	if b.paths == nil {
		return
	}
	data, err := os.ReadFile(b.paths.MessengerHistoryFile())
	if err != nil {
		return
	}
	var loaded historyFile
	if json.Unmarshal(data, &loaded) != nil || loaded.Version != historyFileVersion {
		return
	}
	b.hist = loaded
}

// saveHistoryLocked persists the state with the store's private-file writer
// (0600 temporary file, atomic rename). Caller holds b.histMu.
func (b *Backend) saveHistoryLocked() error {
	if b.paths == nil {
		return nil
	}
	b.hist.Version = historyFileVersion
	b.mu.RLock()
	b.hist.Cutovers = make(map[string]cutoverLink, len(b.cutovers))
	for thread, link := range b.cutovers {
		b.hist.Cutovers[strconv.FormatInt(thread, 10)] = link
	}
	b.mu.RUnlock()
	data, err := json.Marshal(b.hist)
	if err != nil {
		return err
	}
	return appStore.WritePrivateJSON(b.paths.MessengerHistoryFile(), data)
}

// confirmedDevice returns the enrolled backup device, or nil.
func (b *Backend) confirmedDevice() *securehistory.Device {
	b.histMu.Lock()
	defer b.histMu.Unlock()
	b.loadHistoryLocked()
	if d := b.hist.Device; d != nil && d.ID != "" {
		return d
	}
	return nil
}

// anyDevice returns the stored device even when enrollment is unconfirmed.
func (b *Backend) anyDevice() *securehistory.Device {
	b.histMu.Lock()
	defer b.histMu.Unlock()
	b.loadHistoryLocked()
	return b.hist.Device
}

func (b *Backend) threadHistoryState(key string) threadHistory {
	b.histMu.Lock()
	defer b.histMu.Unlock()
	b.loadHistoryLocked()
	if th := b.hist.Threads[key]; th != nil {
		return *th
	}
	return threadHistory{}
}

func (b *Backend) updateThreadHistory(key string, update func(*threadHistory)) {
	b.histMu.Lock()
	defer b.histMu.Unlock()
	b.loadHistoryLocked()
	if b.hist.Threads == nil {
		b.hist.Threads = make(map[string]*threadHistory)
	}
	th := b.hist.Threads[key]
	if th == nil {
		th = &threadHistory{}
		b.hist.Threads[key] = th
	}
	update(th)
	if err := b.saveHistoryLocked(); err != nil {
		b.log.Warn().Err(err).Msg("Could not save Messenger history progress")
	}
}

// saveHistoryEpochs keeps epoch keys the archive derived while paging, so a
// later request can read newer backups. See mautrix-meta saveHistoryEpochs.
func (b *Backend) saveHistoryEpochs(fetched *securehistory.Device, epochs *securehistory.EpochDerivationSet) (*securehistory.Device, error) {
	if epochs == nil || len(epochs.Edges) == 0 {
		return fetched, nil
	}
	b.histMu.Lock()
	defer b.histMu.Unlock()
	previous := b.hist.Device
	if previous == nil || previous.ID != fetched.ID {
		return nil, errors.New("history enrollment changed while fetching messages")
	}
	device, err := previous.WithEpochs(epochs)
	if err != nil {
		return nil, err
	}
	b.hist.Device = device
	if err = b.saveHistoryLocked(); err != nil {
		b.hist.Device = previous
		return nil, err
	}
	return device, nil
}

// setHistoryRestore replaces the Messenger history state and publishes it.
func (b *Backend) setHistoryRestore(h wire.HistoryRestore) {
	b.mu.Lock()
	b.status.HistoryRestore = &h
	b.mu.Unlock()
	if b.publish != nil {
		b.publish(wire.Event{Event: wire.EventStatus, Network: wire.NetworkMessenger, Data: b.Status()})
	}
}

func (b *Backend) currentHistoryRestore() wire.HistoryRestore {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.status.HistoryRestore == nil {
		return wire.HistoryRestore{}
	}
	return *b.status.HistoryRestore
}

// initHistoryStatus reflects persisted state in the status after a connect.
func (b *Backend) initHistoryStatus() {
	b.histMu.Lock()
	b.loadHistoryLocked()
	device := b.hist.Device
	b.histMu.Unlock()
	switch {
	case device != nil && device.ID != "":
		b.mu.Lock()
		b.status.HistoryRestore = &wire.HistoryRestore{State: wire.HistoryRestored, Text: textHistoryRestored, Enrolled: true}
		b.mu.Unlock()
	case device != nil:
		b.mu.Lock()
		b.status.HistoryRestore = &wire.HistoryRestore{State: wire.HistoryUnconfirmed, Text: textHistoryUnconfirmed}
		b.mu.Unlock()
	}
}

// restoreResult maps a recovery failure to a user-facing outcome. It is the
// single place that decides between a wrong PIN, a locked PIN and a failure,
// and it never copies provider text.
func restoreResult(err error) wire.HistoryRestore {
	var recovery *securehistory.RecoveryError
	switch {
	case errors.Is(err, securehistory.ErrNoPINBackup):
		return wire.HistoryRestore{State: wire.HistoryNoBackup, Text: textHistoryNoBackup}
	case errors.As(err, &recovery) && recovery.AttemptsRemaining != nil:
		remaining := *recovery.AttemptsRemaining
		if remaining <= 0 {
			zero := 0
			return wire.HistoryRestore{State: wire.HistoryPINLocked, Text: textHistoryLocked, AttemptsRemaining: &zero}
		}
		return wire.HistoryRestore{
			State: wire.HistoryWrongPIN, AttemptsRemaining: &remaining,
			Text: "That PIN was not accepted. Messenger reports " + attemptsText(remaining) + " remaining.",
		}
	case errors.As(err, &recovery):
		return wire.HistoryRestore{State: wire.HistoryFailed, Text: textHistoryPINFailed}
	case errors.Is(err, metaTypes.ErrPleaseLogIn), errors.Is(err, httpclient.ErrTokenInvalidated):
		return wire.HistoryRestore{State: wire.HistoryFailed, Text: textHistorySession}
	}
	return wire.HistoryRestore{State: wire.HistoryFailed, Text: textHistoryNetwork}
}

func attemptsText(n int) string {
	if n == 1 {
		return "1 attempt"
	}
	return strconv.Itoa(n) + " attempts"
}

// HistoryStatus reports whether the account has an encrypted backup. It uses
// the PIN for nothing and sends one read-only request, at most once per
// session, while the state is still unknown.
func (b *Backend) HistoryStatus(ctx context.Context) (wire.HistoryRestore, error) {
	current := b.currentHistoryRestore()
	if current.State != "" || b.confirmedDevice() != nil {
		return b.finishStatus(current), nil
	}
	service := b.historyClient()
	if service == nil {
		return current, nil
	}
	b.histMu.Lock()
	checked := b.backupChecked
	b.backupChecked = true
	b.histMu.Unlock()
	if checked {
		return current, nil
	}
	_, err := service.GetBackup(ctx)
	switch {
	case errors.Is(err, securehistory.ErrNoPINBackup):
		current = wire.HistoryRestore{State: wire.HistoryNoBackup, Text: textHistoryNoBackup}
	case err != nil:
		b.log.Debug().Err(err).Msg("Could not check for a Messenger encrypted backup")
		b.histMu.Lock()
		b.backupChecked = false
		b.histMu.Unlock()
		return current, nil
	default:
		current = wire.HistoryRestore{State: wire.HistoryAvailable, Text: textHistoryAvailable}
	}
	b.setHistoryRestore(current)
	return current, nil
}

func (b *Backend) finishStatus(h wire.HistoryRestore) wire.HistoryRestore {
	h.Enrolled = b.confirmedDevice() != nil
	return h
}

// RestoreHistory enrolls this desktop with the account's encrypted backup
// using the secure-storage PIN, then starts restoring chats in the background.
//
// It makes exactly one PIN attempt per call and never retries: Meta counts
// wrong PINs and locks the backup when they run out. The PIN is validated
// locally before anything is sent, and the helper clears its copy as soon as
// Meta has answered. When the desktop is already enrolled the PIN is not
// used at all and the call only resumes the restore.
func (b *Backend) RestoreHistory(ctx context.Context, pin []byte) (wire.HistoryRestore, error) {
	defer clear(pin)
	b.mu.RLock()
	cli, e2ee, self := b.client, b.e2eeClient, b.selfID
	b.mu.RUnlock()
	service := b.historyClient()
	if service == nil || (cli == nil && b.newHistory == nil) {
		return wire.HistoryRestore{}, ErrNotConfigured
	}
	if existing := b.anyDevice(); existing != nil {
		if existing.ID == "" {
			h := wire.HistoryRestore{State: wire.HistoryUnconfirmed, Text: textHistoryUnconfirmed}
			b.setHistoryRestore(h)
			return h, nil
		}
		b.startHistoryBackfill()
		return b.finishStatus(b.currentHistoryRestore()), nil
	}
	if !historyPINPattern.Match(pin) {
		return wire.HistoryRestore{State: wire.HistoryFailed, Text: textHistoryPINFormat}, nil
	}
	if e2ee == nil || e2ee.Store == nil || e2ee.Store.IdentityKey == nil {
		return wire.HistoryRestore{State: wire.HistoryFailed, Text: textHistoryNotReady}, nil
	}
	b.histMu.Lock()
	if b.restoreBusy || b.backfilling {
		b.histMu.Unlock()
		return wire.HistoryRestore{State: wire.HistoryRestoring, Text: textHistoryBusy}, nil
	}
	b.restoreBusy = true
	b.histMu.Unlock()
	defer func() {
		b.histMu.Lock()
		b.restoreBusy = false
		b.histMu.Unlock()
	}()

	fail := func(h wire.HistoryRestore) (wire.HistoryRestore, error) {
		b.setHistoryRestore(h)
		return h, nil
	}
	b.setHistoryRestore(wire.HistoryRestore{State: wire.HistoryRestoring, Text: textHistoryChecking})
	backup, err := service.GetBackup(ctx)
	if err != nil {
		b.log.Warn().Err(err).Msg("Could not check Messenger encrypted history")
		return fail(restoreResult(err))
	}
	b.setHistoryRestore(wire.HistoryRestore{State: wire.HistoryRestoring, Text: textHistoryVerifying})
	recovery, err := service.RecoverPIN(ctx, backup, pin)
	clear(pin)
	if err != nil {
		b.log.Warn().Err(err).Msg("Messenger did not accept the history PIN")
		return fail(restoreResult(err))
	}
	b.setHistoryRestore(wire.HistoryRestore{State: wire.HistoryRestoring, Text: textHistoryEnrolling})
	enrollment, err := service.Enroll(ctx, backup, self, recovery.Code, e2ee.Store.IdentityKey)
	clear(recovery.Code)
	if err != nil {
		b.log.Warn().Err(err).Msg("Could not prepare the Messenger history device")
		result := restoreResult(err)
		if result.State == wire.HistoryFailed && result.Text == textHistoryNetwork {
			result.Text = textHistoryReadFailed
		}
		return fail(result)
	}
	// Persist the unconfirmed device first, as mautrix-meta does. If the
	// process dies between Meta accepting the device and the response
	// arriving, the next run sees an unconfirmed device and does not
	// register a second one.
	b.histMu.Lock()
	b.hist.Device = enrollment.Device
	if err = b.saveHistoryLocked(); err != nil {
		b.hist.Device = nil
		b.histMu.Unlock()
		b.log.Warn().Err(err).Msg("Could not save the Messenger history device")
		return fail(wire.HistoryRestore{State: wire.HistoryFailed, Text: "Could not save the history keys in the private data folder. Check its permissions and try again."})
	}
	b.histMu.Unlock()
	device, err := service.AddDevice(ctx, enrollment)
	if err != nil {
		var rejected *securehistory.EnrollmentError
		if errors.As(err, &rejected) {
			b.histMu.Lock()
			b.hist.Device = nil
			_ = b.saveHistoryLocked()
			b.histMu.Unlock()
			b.log.Warn().Err(err).Msg("Messenger rejected the history device")
			return fail(wire.HistoryRestore{State: wire.HistoryFailed, Text: textHistoryRejected})
		}
		b.log.Warn().Err(err).Msg("Messenger did not confirm the history device")
		return fail(wire.HistoryRestore{State: wire.HistoryUnconfirmed, Text: textHistoryUnconfirmed})
	}
	b.histMu.Lock()
	b.hist.Device = device
	err = b.saveHistoryLocked()
	b.histMu.Unlock()
	if err != nil {
		b.log.Warn().Err(err).Msg("Could not save the confirmed Messenger history device")
	}
	b.log.Info().Msg("Enrolled Messenger encrypted history device")
	b.setHistoryRestore(wire.HistoryRestore{State: wire.HistoryRestoring, Text: "Restoring encrypted chats...", Enrolled: true})
	b.startHistoryBackfill()
	return b.finishStatus(b.currentHistoryRestore()), nil
}

// startHistoryBackfill restores the first page of every encrypted chat in the
// background. It is idempotent while a run is active.
func (b *Backend) startHistoryBackfill() {
	b.mu.RLock()
	ctx := b.runCtx
	b.mu.RUnlock()
	if ctx == nil {
		ctx = context.Background()
	}
	b.histMu.Lock()
	if b.backfilling {
		b.histMu.Unlock()
		return
	}
	b.backfilling = true
	b.histMu.Unlock()
	b.backfillDone = make(chan struct{})
	done := b.backfillDone
	go func() {
		defer close(done)
		defer func() {
			b.histMu.Lock()
			b.backfilling = false
			b.histMu.Unlock()
		}()
		b.backfillEncryptedHistory(ctx)
	}()
}

type historyTarget struct {
	thread int64
	jid    waTypes.JID
}

// encryptedTargets lists the chats whose history can be restored, newest
// first. Only chats with a known encrypted address qualify; a stand-in thread
// that was folded into a real one has no mapping any more.
func (b *Backend) encryptedTargets() []historyTarget {
	b.mu.RLock()
	defer b.mu.RUnlock()
	targets := make([]historyTarget, 0, len(b.threadToJID))
	for thread, jid := range b.threadToJID {
		if jid.IsEmpty() || !b.threadTypes[thread].IsWhatsApp() {
			continue
		}
		targets = append(targets, historyTarget{thread: thread, jid: jid})
	}
	sort.Slice(targets, func(i, j int) bool {
		a, c := b.convs[strconv.FormatInt(targets[i].thread, 10)], b.convs[strconv.FormatInt(targets[j].thread, 10)]
		if a.Timestamp != c.Timestamp {
			return a.Timestamp > c.Timestamp
		}
		return targets[i].thread < targets[j].thread
	})
	return targets
}

// backfillEncryptedHistory is runE2EEThreadBackfill plus a first archive page
// per chat. Anything older than that first page loads on demand.
func (b *Backend) backfillEncryptedHistory(ctx context.Context) {
	progress := func(done, total int, text string) {
		b.setHistoryRestore(wire.HistoryRestore{State: wire.HistoryRestoring, Text: text, Done: done, Total: total, Enrolled: true})
	}
	progress(0, 0, "Finding your encrypted chats...")
	if err := b.listEncryptedThreads(ctx); err != nil && ctx.Err() == nil {
		// The chats already known are still restored.
		b.log.Warn().Err(err).Msg("Could not page through every encrypted chat")
	}
	targets := b.encryptedTargets()
	restored, failed := 0, 0
	for i, target := range targets {
		if ctx.Err() != nil {
			b.setHistoryRestore(wire.HistoryRestore{State: wire.HistoryFailed, Text: textHistoryInterrupted, Done: i, Total: len(targets), Enrolled: true})
			return
		}
		progress(i, len(targets), "Restoring encrypted chat "+strconv.Itoa(i+1)+" of "+strconv.Itoa(len(targets))+"...")
		key := strconv.FormatInt(target.thread, 10)
		if b.threadHistoryState(key).Fetched {
			restored++
			continue
		}
		if _, err := b.fetchArchivePage(ctx, target.thread, target.jid, time.Now().Add(time.Minute).UnixMilli()); err != nil {
			failed++
			b.log.Warn().Err(err).Msg("Could not restore an encrypted chat")
		} else {
			restored++
		}
		select {
		case <-time.After(historyThreadPause):
		case <-ctx.Done():
		}
	}
	text := "Restored encrypted history for " + strconv.Itoa(restored) + " of " + strconv.Itoa(len(targets)) + " chats. Older messages load as you scroll."
	if len(targets) == 0 {
		text = "No encrypted chats were found yet. Older messages load when you open a chat."
	}
	if failed > 0 {
		text += " " + strconv.Itoa(failed) + " could not be read; open them to try again."
	}
	b.setHistoryRestore(wire.HistoryRestore{State: wire.HistoryRestored, Text: text, Done: restored, Total: len(targets), Enrolled: true})
	b.publishSnapshots()
}

// listEncryptedThreads pages the encrypted thread list (sync group 95) to its
// end, as mautrix-meta's runE2EEThreadBackfill does, so chats older than the
// first page are known before their history is restored.
func (b *Backend) listEncryptedThreads(ctx context.Context) error {
	if b.threadLister != nil {
		return b.threadLister(ctx)
	}
	b.histMu.Lock()
	b.loadHistoryLocked()
	done := b.hist.ThreadListDone
	b.histMu.Unlock()
	b.mu.RLock()
	cli := b.client
	b.mu.RUnlock()
	if done || cli == nil {
		return nil
	}
	wait, cancel := context.WithTimeout(ctx, historyThreadListWait)
	err := cli.WaitForInitialThreadRange(wait, e2eeSyncGroup)
	cancel()
	if err != nil {
		return err
	}
	for batch := 0; batch < historyThreadListBatches; batch++ {
		keyStore := cli.GetSyncGroupKeyStore(e2eeSyncGroup)
		if keyStore == nil || !keyStore.HasMoreBefore {
			break
		}
		previous := keyStore.MinThreadKey
		generation := cli.ThreadRangeGeneration(e2eeSyncGroup)
		_, tbl, err := cli.FetchMoreThreads(ctx, e2eeSyncGroup)
		if err != nil {
			return err
		}
		if tbl != nil {
			b.handleTable(tbl)
		}
		wait, cancel = context.WithTimeout(ctx, historyThreadListWait)
		err = cli.WaitForThreadRange(wait, e2eeSyncGroup, generation)
		cancel()
		if err != nil {
			return err
		}
		if keyStore.HasMoreBefore && keyStore.MinThreadKey == previous {
			break
		}
		select {
		case <-time.After(historyThreadPause):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	b.histMu.Lock()
	b.hist.ThreadListDone = true
	err = b.saveHistoryLocked()
	b.histMu.Unlock()
	return err
}

// fetchArchivePage reads one archive page older than timestampMS, stores its
// displayable messages and advances the chat's cursor. It returns how many
// messages were new.
func (b *Backend) fetchArchivePage(ctx context.Context, thread int64, jid waTypes.JID, timestampMS int64) (int, error) {
	service := b.historyClient()
	device := b.confirmedDevice()
	if service == nil || device == nil {
		return 0, errors.New("encrypted history is not restored")
	}
	b.mu.RLock()
	self := b.selfID
	b.mu.RUnlock()
	ctx = b.log.WithContext(ctx)
	page, err := service.FetchMessages(ctx, device, jid.User, true, timestampMS, historyPageSize)
	if err != nil {
		return 0, err
	}
	next := page.Range.Before
	if page.Range.HasMoreBefore != nil && *page.Range.HasMoreBefore && next >= timestampMS {
		return 0, errors.New("history cursor did not move backwards")
	}
	device, err = b.saveHistoryEpochs(device, page.Epochs)
	if err != nil {
		return 0, err
	}
	decrypted := service.Decrypt(ctx, page, device, jid.User, self, jid.Server == waTypes.GroupServer)
	added := b.storeArchiveMessages(thread, jid, self, decrypted)
	hasMore := page.Range.HasMoreBefore != nil && *page.Range.HasMoreBefore
	b.updateThreadHistory(strconv.FormatInt(thread, 10), func(th *threadHistory) {
		if !th.Fetched || (next > 0 && (th.Before == 0 || next < th.Before)) {
			th.Before = next
		}
		th.Fetched = true
		th.ArchiveDone = !hasMore
	})
	return added, nil
}

// storeArchiveMessages converts and stores a decrypted page. It does not move
// the conversation's preview or unread state: archived messages are history.
func (b *Backend) storeArchiveMessages(thread int64, jid waTypes.JID, self int64, messages []*securehistory.DecryptedMessage) int {
	converted := make([]*archiveMessage, 0, len(messages))
	for _, message := range messages {
		am, err := convertArchiveMessage(jid, self, message)
		if err != nil {
			// Only the message ID is logged, never its content.
			id := ""
			if message != nil {
				id = message.ID
			}
			b.log.Warn().Err(err).Str("message_id", id).Msg("Could not convert an archived Messenger message")
			continue
		} else if am != nil {
			converted = append(converted, am)
		}
	}
	b.mu.Lock()
	added := 0
	for _, am := range converted {
		if b.storeArchiveMessageLocked(thread, am) {
			added++
		}
	}
	if added > 0 {
		b.saveStoredMessengerDataLocked()
	}
	b.mu.Unlock()
	return added
}

func (b *Backend) storeArchiveMessageLocked(thread int64, am *archiveMessage) bool {
	if _, _, exists := b.messageIndexLocked(thread, am.id); exists {
		return false
	}
	text, media := am.text, am.media
	if am.restricted {
		text, media = lifetimeRestrictedText, nil
	}
	if strings.TrimSpace(text) == "" && media == nil {
		text = unsupportedMessageText
	}
	var attachments []wire.Attachment
	if media != nil {
		media.key = messengerMediaKey(am.id, "e2ee", 0)
		b.media[media.key] = media
		attachments = []wire.Attachment{media.attachment()}
	}
	b.addMessageWithAttachmentsLocked(thread, am.id, text, am.timestampMS, am.sender, false, "", attachments)
	for actor, emoji := range am.reactions {
		b.setActorReactionLocked(thread, am.id, actor, emoji)
	}
	return true
}

// recordCutoverLinksLocked remembers which unencrypted thread an encrypted
// chat replaced. Caller holds b.mu.
func (b *Backend) recordCutoverLinksLocked(links []cutoverLinkRow) {
	for _, link := range links {
		if link.armadilloThread == 0 || link.openThread == 0 {
			continue
		}
		b.cutovers[link.armadilloThread] = cutoverLink{OpenThread: link.openThread, ShowHistory: link.show}
	}
}

type cutoverLinkRow struct {
	armadilloThread, openThread int64
	show                        bool
}

// openThreadPending reports whether stitching the pre-encryption thread can
// still add history to an encrypted chat.
func (b *Backend) openThreadPending(key string, thread int64, th threadHistory) bool {
	if th.OpenDone {
		return false
	}
	b.mu.RLock()
	link, ok := b.cutovers[thread]
	b.mu.RUnlock()
	return ok && link.ShowHistory && link.OpenThread != 0
}

// stitchOpenPage fetches one page of the unencrypted thread that preceded an
// encrypted chat (mautrix-meta's pre-encryption history) and copies it into
// the encrypted chat. It marks the link done when nothing new arrives.
func (b *Backend) stitchOpenPage(ctx context.Context, thread int64) error {
	key := strconv.FormatInt(thread, 10)
	b.mu.RLock()
	link, ok := b.cutovers[thread]
	cli := b.client
	var oldest *wireMessageRef
	if list := b.messages[strconv.FormatInt(link.OpenThread, 10)]; len(list) > 0 {
		oldest = &wireMessageRef{id: list[0].ID, timestampMS: list[0].Timestamp / 1000}
	}
	b.mu.RUnlock()
	if !ok || !link.ShowHistory || link.OpenThread == 0 {
		b.updateThreadHistory(key, func(th *threadHistory) { th.OpenDone = true })
		return nil
	}
	if b.openFetcher != nil {
		if err := b.openFetcher(ctx, link.OpenThread, oldest); err != nil {
			return err
		}
	} else if cli != nil {
		if err := b.fetchOpenThreadPage(ctx, cli, link.OpenThread, oldest); err != nil {
			return err
		}
	} else {
		return ErrNotConfigured
	}
	added := b.copyOpenThread(thread, link.OpenThread)
	if added == 0 {
		b.updateThreadHistory(key, func(th *threadHistory) { th.OpenDone = true })
	}
	return nil
}

// copyOpenThread files the unencrypted thread's messages under the encrypted
// chat, keeping IDs and media references, and returns how many were new.
func (b *Backend) copyOpenThread(thread, open int64) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	key, openKey := strconv.FormatInt(thread, 10), strconv.FormatInt(open, 10)
	have := make(map[string]bool, len(b.messages[key]))
	for _, m := range b.messages[key] {
		have[m.ID] = true
	}
	added := 0
	for _, m := range b.messages[openKey] {
		if m.ID == "" || have[m.ID] {
			continue
		}
		m.ConversationID = key
		b.messages[key] = append(b.messages[key], m)
		have[m.ID] = true
		added++
	}
	if added > 0 {
		list := b.messages[key]
		sort.Slice(list, func(i, j int) bool { return list[i].Timestamp < list[j].Timestamp })
		b.saveStoredMessengerDataLocked()
	}
	return added
}

type wireMessageRef struct {
	id          string
	timestampMS int64
}

// encryptedMessages answers the messages method for an encrypted chat whose
// backup is restored. A request without a cursor returns the newest messages
// (reading the first archive page if that never happened); a request with a
// cursor returns the page older than it, reading the archive, and then the
// pre-encryption thread, only as far as needed.
func (b *Backend) encryptedMessages(ctx context.Context, thread int64, p wire.MessagesParams) (wire.MessagesResult, error) {
	key := p.ConversationID
	count := int(p.Count)
	if count <= 0 || count > 200 {
		count = 60
	}
	b.mu.RLock()
	jid := b.threadToJID[thread]
	b.mu.RUnlock()
	older := p.CursorID != "" || p.CursorTime > 0
	var fetchErr error
	if !jid.IsEmpty() {
		if !older {
			if !b.threadHistoryState(key).Fetched {
				_, fetchErr = b.fetchArchivePage(ctx, thread, jid, time.Now().Add(time.Minute).UnixMilli())
			}
		} else {
			fetchErr = b.fillOlder(ctx, thread, jid, p.CursorTime, count)
		}
	}
	cursor := int64(0)
	if older {
		cursor = p.CursorTime
	}
	page, moreLocal := b.localPage(key, cursor, count)
	th := b.threadHistoryState(key)
	more := moreLocal || (!jid.IsEmpty() && (!th.ArchiveDone || b.openThreadPending(key, thread, th)))
	result := wire.MessagesResult{ConversationID: key, Messages: page, HasMore: more}
	if more {
		result.CursorID = "e2ee-history"
		switch {
		case len(page) > 0:
			result.CursorTime = page[0].Timestamp / 1000
		case th.Before > 0:
			result.CursorTime = th.Before
		default:
			result.CursorTime = cursor
		}
		if result.CursorTime <= 0 {
			result.CursorTime = 1
		}
	}
	if fetchErr != nil {
		b.log.Warn().Err(fetchErr).Msg("Could not read encrypted Messenger history")
		if older {
			return result, errors.New("Could not load older encrypted messages. Try again.")
		}
		result.HistoryNotice = "Older encrypted messages could not be loaded right now. Open the chat again to retry."
	}
	return result, nil
}

// localPage returns the newest count stored messages older than cursorMS (all
// of them when cursorMS is 0) in time order, and whether older ones remain.
func (b *Backend) localPage(key string, cursorMS int64, count int) ([]wire.Message, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	list := b.messages[key]
	end := len(list)
	if cursorMS > 0 {
		limit := messengerTimestamp(cursorMS)
		end = sort.Search(len(list), func(i int) bool { return list[i].Timestamp >= limit })
	}
	start := max(0, end-count)
	// Never split messages that share a timestamp: the next cursor is the
	// page's oldest time, and a tie left behind would be skipped.
	for start > 0 && start < len(list) && list[start-1].Timestamp == list[start].Timestamp {
		start--
	}
	return append([]wire.Message(nil), list[start:end]...), start > 0
}

func (b *Backend) olderLocalCount(key string, cursorMS int64) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	list := b.messages[key]
	limit := messengerTimestamp(cursorMS)
	return sort.Search(len(list), func(i int) bool { return list[i].Timestamp >= limit })
}

// fillOlder makes sure enough messages older than cursorMS are stored. It
// reads the archive from the chat's frontier, and once the archive is
// exhausted, the pre-encryption thread.
func (b *Backend) fillOlder(ctx context.Context, thread int64, jid waTypes.JID, cursorMS int64, count int) error {
	key := strconv.FormatInt(thread, 10)
	for range historyMaxPagesPerRequest {
		if b.olderLocalCount(key, cursorMS) >= count {
			return nil
		}
		th := b.threadHistoryState(key)
		switch {
		case !th.ArchiveDone:
			from := cursorMS
			if th.Fetched && th.Before > 0 && th.Before < from {
				from = th.Before
			}
			if _, err := b.fetchArchivePage(ctx, thread, jid, from); err != nil {
				return err
			}
		case b.openThreadPending(key, thread, th):
			if err := b.stitchOpenPage(ctx, thread); err != nil {
				return err
			}
		default:
			return nil
		}
	}
	return nil
}

// encryptedHistoryRestorable reports whether the panel should offer to
// restore history for an encrypted chat.
func (b *Backend) encryptedHistoryRestorable() bool {
	state := b.currentHistoryRestore().State
	return state != wire.HistoryNoBackup && b.confirmedDevice() == nil
}
