package messenger

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	appStore "github.com/onelegdave/omachat/internal/store"
	"github.com/onelegdave/omachat/internal/wire"
	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	metaTypes "go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waArmadilloBackupCommon"
	"go.mau.fi/whatsmeow/proto/waArmadilloBackupMessage"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	waStore "go.mau.fi/whatsmeow/store"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	"google.golang.org/protobuf/proto"
)

const testPIN = "135790"

// fakeHistory replaces every call that would reach Meta. It records calls and
// never sees real data.
type fakeHistory struct {
	mu sync.Mutex

	backupErr   error
	recoverErr  error
	enrollErr   error
	addErr      error
	canUpload   bool
	deviceID    string
	pinsTried   [][]byte
	pinCopies   []string
	recoverCall int
	enrollCall  int
	addCall     int
	fetchCalls  []fetchCall
	uploads     []*securehistory.MinosSentMessage
	// pages answers an archive request for a thread.
	pages func(thread string, ts int64) (before int64, hasMore bool, msgs []*securehistory.DecryptedMessage, err error)

	decrypted map[*securehistory.MessageRange][]*securehistory.DecryptedMessage
}

type fetchCall struct {
	thread string
	ts     int64
}

func (f *fakeHistory) GetBackup(context.Context) (*securehistory.Backup, error) {
	if f.backupErr != nil {
		return nil, f.backupErr
	}
	return &securehistory.Backup{ID: "backup-1"}, nil
}

func (f *fakeHistory) RecoverPIN(_ context.Context, _ *securehistory.Backup, pin []byte) (*securehistory.Recovery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recoverCall++
	f.pinCopies = append(f.pinCopies, string(pin))
	f.pinsTried = append(f.pinsTried, pin)
	if f.recoverErr != nil {
		return nil, f.recoverErr
	}
	return &securehistory.Recovery{Code: []byte("1234567890123456789012345678901234567890")}, nil
}

func (f *fakeHistory) Enroll(_ context.Context, backup *securehistory.Backup, _ int64, code []byte, _ *keys.KeyPair) (*securehistory.Enrollment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enrollCall++
	if f.enrollErr != nil {
		return nil, f.enrollErr
	}
	return &securehistory.Enrollment{Device: &securehistory.Device{
		BackupID: backup.ID, MailboxRootKey: []byte("mailbox-key"), OCMFClientState: []byte("ocmf"),
		StoragePrivateKey: []byte("storage-private"),
		Epochs:            map[string]securehistory.Epoch{"e1": {ID: "e1", AnonID: []byte("1"), RootKey: make([]byte, 32)}},
	}}, nil
}

func (f *fakeHistory) AddDevice(_ context.Context, enrollment *securehistory.Enrollment) (*securehistory.Device, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addCall++
	if f.addErr != nil {
		return nil, f.addErr
	}
	device := *enrollment.Device
	device.ID = f.deviceID
	if device.ID == "" {
		device.ID = "device-1"
	}
	return &device, nil
}

func (f *fakeHistory) FetchMessages(_ context.Context, device *securehistory.Device, threadID string, before bool, ts int64, count int) (*securehistory.MessageRange, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetchCalls = append(f.fetchCalls, fetchCall{threadID, ts})
	if device == nil || device.ID == "" || !before || count <= 0 {
		return nil, errors.New("unexpected archive request")
	}
	if f.pages == nil {
		more, after := false, false
		return &securehistory.MessageRange{Range: &securehistory.MessageRangeInfo{HasMoreBefore: &more, HasMoreAfter: &after}}, nil
	}
	nextBefore, hasMore, msgs, err := f.pages(threadID, ts)
	if err != nil {
		return nil, err
	}
	after := false
	page := &securehistory.MessageRange{Range: &securehistory.MessageRangeInfo{HasMoreBefore: &hasMore, HasMoreAfter: &after, Before: nextBefore}}
	if f.decrypted == nil {
		f.decrypted = make(map[*securehistory.MessageRange][]*securehistory.DecryptedMessage)
	}
	f.decrypted[page] = msgs
	return page, nil
}

func (f *fakeHistory) Decrypt(_ context.Context, page *securehistory.MessageRange, _ *securehistory.Device, _ string, _ int64, _ bool) []*securehistory.DecryptedMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.decrypted[page]
}

func (f *fakeHistory) UploadSentMessage(_ context.Context, msg *securehistory.MinosSentMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.uploads = append(f.uploads, msg)
	return nil
}

func (f *fakeHistory) CanUpload() bool { return f.canUpload }

func (f *fakeHistory) fetches() []fetchCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fetchCall(nil), f.fetchCalls...)
}

// archiveBackup builds a decrypted archive message carrying a consumer
// application, the same shape securehistory hands back.
func archiveBackup(t *testing.T, id string, sender int64, tsMS int64, content *waConsumerApplication.ConsumerApplication_Content) *waArmadilloBackupMessage.BackupMessage {
	t.Helper()
	app := &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
		Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{Content: content},
	}}
	data, err := marshalSentFBMessage(app, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &waArmadilloBackupMessage.BackupMessage{
		Metadata: &waArmadilloBackupCommon.Metadata{
			SenderID: proto.String(strconv.FormatInt(sender, 10)), MessageID: proto.String(id), TimestampMS: proto.Int64(tsMS),
		},
		Payload: &waArmadilloBackupMessage.BackupMessage_EncryptedTransportMessage{EncryptedTransportMessage: data},
	}
}

func textContent(text string) *waConsumerApplication.ConsumerApplication_Content {
	return &waConsumerApplication.ConsumerApplication_Content{Content: &waConsumerApplication.ConsumerApplication_Content_MessageText{
		MessageText: &waCommon.MessageText{Text: proto.String(text)},
	}}
}

func archiveText(t *testing.T, id string, sender, tsMS int64, text string) *securehistory.DecryptedMessage {
	t.Helper()
	return &securehistory.DecryptedMessage{
		ID:      id,
		Message: &securehistory.DecryptedProtobuf{Message: archiveBackup(t, id, sender, tsMS, textContent(text)), TimestampMS: tsMS},
	}
}

type testEvents struct {
	mu     sync.Mutex
	events []wire.Event
}

func (e *testEvents) publish(evt wire.Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, evt)
}

func (e *testEvents) states() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []string
	for _, evt := range e.events {
		if st, ok := evt.Data.(wire.Status); ok && st.HistoryRestore != nil {
			out = append(out, st.HistoryRestore.State)
		}
	}
	return out
}

// newHistoryBackend builds a connected-looking backend whose Meta calls go to
// the fake. Encrypted thread 9 maps to chat 4242 (one-to-one).
func newHistoryBackend(t *testing.T, fake *fakeHistory) (*Backend, *testEvents) {
	t.Helper()
	paths := setupMessengerPaths(t)
	if err := os.MkdirAll(paths.Data, 0o700); err != nil {
		t.Fatal(err)
	}
	events := &testEvents{}
	b := New(zerolog.Nop(), paths, events.publish)
	b.selfID = 1
	b.newHistory = func(*messagix.Client) historyService { return fake }
	b.threadLister = func(context.Context) error { return nil }
	b.e2eeClient = &whatsmeow.Client{Store: &waStore.Device{IdentityKey: keys.NewKeyPair()}}
	b.setMappingLocked(9, 4242, table.ENCRYPTED_OVER_WA_ONE_TO_ONE)
	b.upsertThreadLocked(9, "Friend", "latest live", 5_000_000, 0, false, false, false, "", table.ENCRYPTED_OVER_WA_ONE_TO_ONE)
	return b, events
}

func waitBackfill(t *testing.T, b *Backend) {
	t.Helper()
	if b.backfillDone == nil {
		t.Fatal("no backfill was started")
	}
	select {
	case <-b.backfillDone:
	case <-time.After(10 * time.Second):
		t.Fatal("backfill did not finish")
	}
}

func TestRestoreHistoryEnrollsPersistsAndBackfills(t *testing.T) {
	fake := &fakeHistory{}
	fake.pages = func(thread string, ts int64) (int64, bool, []*securehistory.DecryptedMessage, error) {
		if thread != "4242" {
			t.Fatalf("archive thread = %q, want the chat's JID user", thread)
		}
		return 1_000, true, []*securehistory.DecryptedMessage{
			archiveText(t, "m2", 4242, 3_000, "newer from friend"),
			archiveText(t, "m1", 1, 2_000, "mine, older"),
		}, nil
	}
	b, events := newHistoryBackend(t, fake)
	pin := []byte(testPIN)

	got, err := b.RestoreHistory(context.Background(), pin)
	if err != nil {
		t.Fatal(err)
	}
	waitBackfill(t, b)
	if got.State != wire.HistoryRestoring && got.State != wire.HistoryRestored {
		t.Fatalf("restore state = %q (%q)", got.State, got.Text)
	}
	if fake.recoverCall != 1 || fake.enrollCall != 1 || fake.addCall != 1 {
		t.Fatalf("calls recover=%d enroll=%d add=%d, want exactly one of each", fake.recoverCall, fake.enrollCall, fake.addCall)
	}
	if fake.pinCopies[0] != testPIN {
		t.Fatalf("PIN reached the client as %q", fake.pinCopies[0])
	}
	for _, c := range pin {
		if c != 0 {
			t.Fatalf("the caller's PIN was not cleared: %v", pin)
		}
	}
	for _, kept := range fake.pinsTried {
		for _, c := range kept {
			if c != 0 {
				t.Fatalf("the PIN slice handed to Meta's client was not cleared afterwards")
			}
		}
	}

	if status := b.Status().HistoryRestore; status == nil || status.State != wire.HistoryRestored || !status.Enrolled || status.Done != 1 || status.Total != 1 {
		t.Fatalf("final status = %#v", status)
	}
	states := events.states()
	if len(states) < 3 || states[len(states)-1] != wire.HistoryRestored {
		t.Fatalf("status events = %v", states)
	}

	// Stored in order, with sender and timestamps, without moving the preview.
	list := b.messages["9"]
	if len(list) != 2 || list[0].ID != "m1" || list[1].ID != "m2" {
		t.Fatalf("restored messages = %#v", list)
	}
	if !list[0].FromMe || list[1].FromMe || list[1].SenderID != "4242" || list[1].Timestamp != messengerTimestamp(3_000) {
		t.Fatalf("restored sender or time wrong: %#v", list)
	}
	if conv := b.convs["9"]; conv.Preview != "latest live" || conv.Timestamp != messengerTimestamp(5_000_000) {
		t.Fatalf("restore changed the conversation preview: %#v", conv)
	}

	// The device keys are persisted privately; the PIN is nowhere on disk.
	info, err := os.Stat(b.paths.MessengerHistoryFile())
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("history file mode = %v err %v, want 0600", info, err)
	}
	data, _ := os.ReadFile(b.paths.MessengerHistoryFile())
	if strings.Contains(string(data), testPIN) || strings.Contains(string(data), "1234567890123456789012345678901234567890") {
		t.Fatal("the PIN or recovery code was written to disk")
	}
	var onDisk historyFile
	if err := json.Unmarshal(data, &onDisk); err != nil || onDisk.Device == nil || onDisk.Device.ID != "device-1" || !onDisk.Threads["9"].Fetched {
		t.Fatalf("persisted state = %s (%v)", data, err)
	}
	for _, evt := range events.events {
		encoded, _ := json.Marshal(evt)
		if strings.Contains(string(encoded), testPIN) {
			t.Fatalf("event carries the PIN: %s", encoded)
		}
	}
}

func TestRestoreHistorySurvivesRestartWithoutPIN(t *testing.T) {
	fake := &fakeHistory{}
	fake.pages = func(thread string, ts int64) (int64, bool, []*securehistory.DecryptedMessage, error) {
		return 100, false, []*securehistory.DecryptedMessage{archiveText(t, "old", 4242, 100, "restored text")}, nil
	}
	b, _ := newHistoryBackend(t, fake)
	if _, err := b.RestoreHistory(context.Background(), []byte(testPIN)); err != nil {
		t.Fatal(err)
	}
	waitBackfill(t, b)

	// A new process: same files, fresh memory.
	again := &fakeHistory{}
	restarted := New(zerolog.Nop(), b.paths, nil)
	restarted.newHistory = func(*messagix.Client) historyService { return again }
	restarted.threadLister = func(context.Context) error { return nil }
	restarted.initHistoryStatus()
	if status := restarted.Status().HistoryRestore; status == nil || status.State != wire.HistoryRestored || !status.Enrolled {
		t.Fatalf("status after restart = %#v", status)
	}
	if device := restarted.confirmedDevice(); device == nil || device.ID != "device-1" || len(device.Epochs) != 1 {
		t.Fatalf("device after restart = %#v", device)
	}
	// Restoring again needs no PIN and sends none.
	res, err := restarted.RestoreHistory(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if again.recoverCall != 0 || again.enrollCall != 0 || res.State == wire.HistoryFailed {
		t.Fatalf("restore after restart used the PIN path: recover=%d res=%#v", again.recoverCall, res)
	}
	waitBackfill(t, restarted)
}

func TestRestoreHistoryWrongPINReportsAttemptsAndNeverRetries(t *testing.T) {
	remaining := 2
	fake := &fakeHistory{recoverErr: &securehistory.RecoveryError{Code: "WRONG", AttemptsRemaining: &remaining}}
	b, _ := newHistoryBackend(t, fake)
	got, err := b.RestoreHistory(context.Background(), []byte(testPIN))
	if err != nil {
		t.Fatal(err)
	}
	if got.State != wire.HistoryWrongPIN || got.AttemptsRemaining == nil || *got.AttemptsRemaining != 2 || !strings.Contains(got.Text, "2 attempts") {
		t.Fatalf("wrong PIN result = %#v", got)
	}
	if fake.recoverCall != 1 || fake.enrollCall != 0 || fake.addCall != 0 {
		t.Fatalf("a wrong PIN must be tried once and stop: recover=%d enroll=%d add=%d", fake.recoverCall, fake.enrollCall, fake.addCall)
	}
	if b.confirmedDevice() != nil || b.anyDevice() != nil {
		t.Fatal("a wrong PIN left a device behind")
	}
	if _, err := os.Stat(b.paths.MessengerHistoryFile()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a wrong PIN wrote a history file: %v", err)
	}
}

func TestRestoreHistoryAttemptsExhaustedTellsUserToResetPIN(t *testing.T) {
	zero := 0
	fake := &fakeHistory{recoverErr: &securehistory.RecoveryError{Code: "no attempts remaining", AttemptsRemaining: &zero}}
	b, _ := newHistoryBackend(t, fake)
	got, _ := b.RestoreHistory(context.Background(), []byte(testPIN))
	if got.State != wire.HistoryPINLocked || !strings.Contains(got.Text, "Reset your PIN in the Messenger app") {
		t.Fatalf("locked result = %#v", got)
	}
}

func TestRestoreHistoryNoBackup(t *testing.T) {
	fake := &fakeHistory{backupErr: securehistory.ErrNoPINBackup}
	b, _ := newHistoryBackend(t, fake)
	got, _ := b.RestoreHistory(context.Background(), []byte(testPIN))
	if got.State != wire.HistoryNoBackup || fake.recoverCall != 0 {
		t.Fatalf("no backup result = %#v, recover calls %d", got, fake.recoverCall)
	}
}

func TestRestoreHistoryRejectsMalformedPINBeforeContactingMeta(t *testing.T) {
	for _, pin := range []string{"", "12345", "1234567", "12a456", "１２３４５６"} {
		fake := &fakeHistory{}
		b, _ := newHistoryBackend(t, fake)
		got, err := b.RestoreHistory(context.Background(), []byte(pin))
		if err != nil || got.State != wire.HistoryFailed || fake.recoverCall != 0 {
			t.Fatalf("pin %q: result %#v err %v recover=%d", pin, got, err, fake.recoverCall)
		}
	}
}

func TestRestoreHistoryRequiresConnectedEncryptedTransport(t *testing.T) {
	fake := &fakeHistory{}
	b, _ := newHistoryBackend(t, fake)
	b.e2eeClient = nil
	got, err := b.RestoreHistory(context.Background(), []byte(testPIN))
	if err != nil || got.State != wire.HistoryFailed || fake.recoverCall != 0 {
		t.Fatalf("result %#v err %v recover=%d", got, err, fake.recoverCall)
	}
	unconfigured := New(zerolog.Nop(), nil, nil)
	if _, err := unconfigured.RestoreHistory(context.Background(), []byte(testPIN)); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("unconfigured restore error = %v, want ErrNotConfigured", err)
	}
}

func TestRestoreHistoryEnrollmentRejectedClearsDevice(t *testing.T) {
	fake := &fakeHistory{addErr: &securehistory.EnrollmentError{Code: "NOPE"}}
	b, _ := newHistoryBackend(t, fake)
	got, _ := b.RestoreHistory(context.Background(), []byte(testPIN))
	if got.State != wire.HistoryFailed || b.anyDevice() != nil {
		t.Fatalf("rejected enrollment = %#v, device %#v", got, b.anyDevice())
	}
}

func TestRestoreHistoryUnconfirmedEnrollmentIsNotRepeated(t *testing.T) {
	fake := &fakeHistory{addErr: errors.New("connection reset")}
	b, _ := newHistoryBackend(t, fake)
	got, _ := b.RestoreHistory(context.Background(), []byte(testPIN))
	if got.State != wire.HistoryUnconfirmed {
		t.Fatalf("lost confirmation = %#v", got)
	}
	device := b.anyDevice()
	if device == nil || device.ID != "" || b.confirmedDevice() != nil {
		t.Fatalf("an unconfirmed device must be kept, unconfirmed: %#v", device)
	}
	// A second attempt does not register another device and does not spend a PIN attempt.
	again, _ := b.RestoreHistory(context.Background(), []byte(testPIN))
	if again.State != wire.HistoryUnconfirmed || fake.recoverCall != 1 || fake.addCall != 1 {
		t.Fatalf("second attempt = %#v recover=%d add=%d", again, fake.recoverCall, fake.addCall)
	}
}

func TestRestoreResultMapping(t *testing.T) {
	two, zero := 2, 0
	cases := []struct {
		name  string
		err   error
		state string
	}{
		{"no backup", securehistory.ErrNoPINBackup, wire.HistoryNoBackup},
		{"wrong pin", &securehistory.RecoveryError{Code: "x", AttemptsRemaining: &two}, wire.HistoryWrongPIN},
		{"locked", &securehistory.RecoveryError{Code: "x", AttemptsRemaining: &zero}, wire.HistoryPINLocked},
		{"unknown recovery", &securehistory.RecoveryError{Code: "x"}, wire.HistoryFailed},
		{"signed out", metaTypes.ErrPleaseLogIn, wire.HistoryFailed},
		{"token", httpclient.ErrTokenInvalidated, wire.HistoryFailed},
		{"network", errors.New("dial tcp: boom"), wire.HistoryFailed},
	}
	for _, tc := range cases {
		got := restoreResult(tc.err)
		if got.State != tc.state || got.Text == "" {
			t.Errorf("%s: %#v", tc.name, got)
		}
		if strings.Contains(got.Text, "boom") || strings.Contains(got.Text, "x)") {
			t.Errorf("%s: provider text leaked into %q", tc.name, got.Text)
		}
	}
	if got := restoreResult(&securehistory.RecoveryError{Code: "x", AttemptsRemaining: &two}); got.AttemptsRemaining == nil || *got.AttemptsRemaining != 2 {
		t.Errorf("remaining attempts lost: %#v", got)
	}
	one := 1
	if got := restoreResult(&securehistory.RecoveryError{AttemptsRemaining: &one}); !strings.Contains(got.Text, "1 attempt ") && !strings.Contains(got.Text, "1 attempt remaining") {
		t.Errorf("singular attempts text = %q", got.Text)
	}
}

func TestHistoryStatusChecksBackupOncePerSession(t *testing.T) {
	fake := &fakeHistory{}
	b, _ := newHistoryBackend(t, fake)
	got, err := b.HistoryStatus(context.Background())
	if err != nil || got.State != wire.HistoryAvailable {
		t.Fatalf("status = %#v err %v", got, err)
	}
	if again, _ := b.HistoryStatus(context.Background()); again.State != wire.HistoryAvailable {
		t.Fatalf("second status = %#v", again)
	}

	none := &fakeHistory{backupErr: securehistory.ErrNoPINBackup}
	b, _ = newHistoryBackend(t, none)
	if got, _ := b.HistoryStatus(context.Background()); got.State != wire.HistoryNoBackup {
		t.Fatalf("no backup status = %#v", got)
	}
	// A transient failure leaves the state unknown and allows a later retry.
	flaky := &fakeHistory{backupErr: errors.New("timeout")}
	b, _ = newHistoryBackend(t, flaky)
	if got, _ := b.HistoryStatus(context.Background()); got.State != "" {
		t.Fatalf("failed check = %#v, want unknown", got)
	}
	flaky.backupErr = nil
	if got, _ := b.HistoryStatus(context.Background()); got.State != wire.HistoryAvailable {
		t.Fatalf("retry = %#v", got)
	}
}

func TestEncryptedNoticeChangesOnceRestored(t *testing.T) {
	fake := &fakeHistory{}
	fake.pages = func(string, int64) (int64, bool, []*securehistory.DecryptedMessage, error) {
		return 10, false, []*securehistory.DecryptedMessage{archiveText(t, "a", 4242, 10, "x")}, nil
	}
	b, _ := newHistoryBackend(t, fake)
	b.messages["9"] = []wire.Message{{ID: "live", ConversationID: "9", Text: "live", Timestamp: messengerTimestamp(9_000)}}

	// Before restore: the notice offers restore.
	res, err := b.Messages(context.Background(), wire.MessagesParams{ConversationID: "9", Count: 60})
	if err != nil || res.HistoryNotice != encryptedHistoryNotice || !res.HistoryRestorable || res.HasMore {
		t.Fatalf("before restore: %#v err %v", res, err)
	}
	if !strings.Contains(res.HistoryNotice, "Restore them with your Messenger PIN") || strings.Contains(res.HistoryNotice, "not currently supported") {
		t.Fatalf("notice text = %q", res.HistoryNotice)
	}

	// An account without a backup gets a different, final notice.
	b.setHistoryRestore(wire.HistoryRestore{State: wire.HistoryNoBackup})
	res, _ = b.Messages(context.Background(), wire.MessagesParams{ConversationID: "9", Count: 60})
	if res.HistoryNotice != encryptedNoBackupNotice || res.HistoryRestorable {
		t.Fatalf("no backup: %#v", res)
	}
	b.setHistoryRestore(wire.HistoryRestore{})

	if _, err := b.RestoreHistory(context.Background(), []byte(testPIN)); err != nil {
		t.Fatal(err)
	}
	waitBackfill(t, b)
	res, err = b.Messages(context.Background(), wire.MessagesParams{ConversationID: "9", Count: 60})
	if err != nil {
		t.Fatal(err)
	}
	if res.HistoryNotice != "" || res.HistoryRestorable || res.HasMore {
		t.Fatalf("after restore: notice %q restorable %v hasMore %v", res.HistoryNotice, res.HistoryRestorable, res.HasMore)
	}
	if len(res.Messages) != 2 || res.Messages[0].ID != "a" || res.Messages[1].ID != "live" {
		t.Fatalf("messages after restore = %#v", res.Messages)
	}
}

func TestEncryptedPagingUsesArchiveCursorAndHasMore(t *testing.T) {
	fake := &fakeHistory{}
	// The archive holds messages at 1000..200000 ms, newest first, two pages.
	fake.pages = func(thread string, ts int64) (int64, bool, []*securehistory.DecryptedMessage, error) {
		var out []*securehistory.DecryptedMessage
		first := ts > 186_000
		if first {
			for i := 0; i < 5; i++ {
				ms := int64(190_000 - i*1_000)
				out = append(out, archiveText(t, "new"+strconv.Itoa(i), 4242, ms, "n"))
			}
			return 186_000, true, out, nil
		}
		for i := 0; i < 3; i++ {
			ms := int64(50_000 - i*1_000)
			out = append(out, archiveText(t, "old"+strconv.Itoa(i), 1, ms, "o"))
		}
		return 48_000, false, out, nil
	}
	b, _ := newHistoryBackend(t, fake)
	if _, err := b.RestoreHistory(context.Background(), []byte(testPIN)); err != nil {
		t.Fatal(err)
	}
	waitBackfill(t, b)

	page1, err := b.Messages(context.Background(), wire.MessagesParams{ConversationID: "9", Count: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Messages) != 3 || !page1.HasMore || page1.CursorID == "" || page1.CursorTime != 188_000 {
		t.Fatalf("newest page = %#v", page1)
	}
	if page1.Messages[2].ID != "new0" {
		t.Fatalf("newest page order = %#v", page1.Messages)
	}

	page2, err := b.Messages(context.Background(), wire.MessagesParams{ConversationID: "9", Count: 3, CursorID: page1.CursorID, CursorTime: page1.CursorTime})
	if err != nil {
		t.Fatal(err)
	}
	// The two stored older messages are not a full page, so the archive is read
	// again from its stored frontier (186000, not the cursor) to fill it.
	calls := fake.fetches()
	if last := calls[len(calls)-1]; last.thread != "4242" || last.ts != 186_000 {
		t.Fatalf("archive resumed from %#v, want the stored frontier 186000", last)
	}
	if len(page2.Messages) != 3 || page2.Messages[0].ID != "old0" || page2.Messages[1].ID != "new4" ||
		!page2.HasMore || page2.CursorTime != 50_000 {
		t.Fatalf("second page = %#v", page2)
	}

	// The archive is exhausted; the remaining stored messages end paging.
	page3, err := b.Messages(context.Background(), wire.MessagesParams{ConversationID: "9", Count: 3, CursorID: page2.CursorID, CursorTime: page2.CursorTime})
	if err != nil {
		t.Fatal(err)
	}
	if len(page3.Messages) != 2 || page3.Messages[0].ID != "old2" || page3.Messages[1].ID != "old1" ||
		page3.HasMore || page3.CursorID != "" {
		t.Fatalf("last page = %#v", page3)
	}
	if th := b.threadHistoryState("9"); !th.ArchiveDone || th.Before != 48_000 {
		t.Fatalf("thread cursor = %#v", th)
	}
}

func TestEncryptedPagingStitchesPreEncryptionHistory(t *testing.T) {
	fake := &fakeHistory{}
	fake.pages = func(string, int64) (int64, bool, []*securehistory.DecryptedMessage, error) {
		return 1_000, false, []*securehistory.DecryptedMessage{archiveText(t, "enc", 4242, 1_000_000, "encrypted era")}, nil
	}
	b, _ := newHistoryBackend(t, fake)
	b.handleTable(&table.LSTable{LSCreateOpenToE2EEThreadLink: []*table.LSCreateOpenToE2EEThreadLink{{OpenThreadID: 77, ArmadilloThreadID: 9, ShowOpenMessageHistory: true}}})
	fetches := 0
	b.openFetcher = func(_ context.Context, open int64, oldest *wireMessageRef) error {
		fetches++
		if open != 77 {
			t.Fatalf("open thread = %d", open)
		}
		if fetches == 1 {
			if oldest != nil {
				t.Fatalf("first stitch should have no reference, got %#v", oldest)
			}
			b.handleTable(&table.LSTable{LSUpsertMessage: []*table.LSUpsertMessage{
				{ThreadKey: 77, MessageId: "mid.old1", Text: "before encryption", TimestampMs: 500_000, SenderId: 4242},
			}})
		}
		return nil
	}
	if _, err := b.RestoreHistory(context.Background(), []byte(testPIN)); err != nil {
		t.Fatal(err)
	}
	waitBackfill(t, b)

	page, err := b.Messages(context.Background(), wire.MessagesParams{ConversationID: "9", Count: 5})
	if err != nil {
		t.Fatal(err)
	}
	// The archive is exhausted but the older thread is still pending.
	if !page.HasMore || len(page.Messages) != 1 {
		t.Fatalf("first page = %#v", page)
	}
	older, err := b.Messages(context.Background(), wire.MessagesParams{ConversationID: "9", Count: 5, CursorID: page.CursorID, CursorTime: page.CursorTime})
	if err != nil {
		t.Fatal(err)
	}
	if len(older.Messages) != 1 || older.Messages[0].ID != "mid.old1" || older.Messages[0].ConversationID != "9" || older.Messages[0].Text != "before encryption" {
		t.Fatalf("stitched page = %#v", older)
	}
	// The pre-encryption thread had nothing older, so paging ends here.
	if older.HasMore || older.CursorID != "" {
		t.Fatalf("stitched page should end paging: %#v", older)
	}
	if th := b.threadHistoryState("9"); !th.OpenDone {
		t.Fatalf("thread cursor = %#v", th)
	}
}

func TestCutoverLinksArePersistedWithHistoryState(t *testing.T) {
	fake := &fakeHistory{}
	b, _ := newHistoryBackend(t, fake)
	b.handleTable(&table.LSTable{LSCreateOpenToE2EEThreadLink: []*table.LSCreateOpenToE2EEThreadLink{{OpenThreadID: 77, ArmadilloThreadID: 9, ShowOpenMessageHistory: true}}})
	if _, err := b.RestoreHistory(context.Background(), []byte(testPIN)); err != nil {
		t.Fatal(err)
	}
	waitBackfill(t, b)
	data, _ := os.ReadFile(b.paths.MessengerHistoryFile())
	var onDisk historyFile
	if err := json.Unmarshal(data, &onDisk); err != nil || onDisk.Cutovers["9"] != (cutoverLink{OpenThread: 77, ShowHistory: true}) {
		t.Fatalf("persisted cutovers = %s (%v)", data, err)
	}
}

func TestArchiveFailureKeepsOtherChatsAndLocalMessages(t *testing.T) {
	fake := &fakeHistory{}
	fake.pages = func(thread string, ts int64) (int64, bool, []*securehistory.DecryptedMessage, error) {
		return 0, false, nil, errors.New("boom")
	}
	b, _ := newHistoryBackend(t, fake)
	b.messages["9"] = []wire.Message{{ID: "live", ConversationID: "9", Text: "live", Timestamp: messengerTimestamp(9_000)}}
	if _, err := b.RestoreHistory(context.Background(), []byte(testPIN)); err != nil {
		t.Fatal(err)
	}
	waitBackfill(t, b)
	status := b.Status().HistoryRestore
	if status.State != wire.HistoryRestored || !strings.Contains(status.Text, "1 could not be read") {
		t.Fatalf("status = %#v", status)
	}
	res, err := b.Messages(context.Background(), wire.MessagesParams{ConversationID: "9", Count: 60})
	if err != nil || len(res.Messages) != 1 || res.HistoryNotice == "" || res.HistoryNotice == encryptedHistoryNotice {
		t.Fatalf("messages on archive failure = %#v err %v", res, err)
	}
	if strings.Contains(res.HistoryNotice, "boom") {
		t.Fatal("provider error leaked into the notice")
	}
}

func TestEncryptedThreadListIsPagedBeforeRestore(t *testing.T) {
	fake := &fakeHistory{}
	b, _ := newHistoryBackend(t, fake)
	calls := 0
	b.threadLister = func(context.Context) error {
		calls++
		// An older chat appears while paging the thread list.
		b.mu.Lock()
		b.setMappingLocked(10, 5555, table.ENCRYPTED_OVER_WA_ONE_TO_ONE)
		b.upsertThreadLocked(10, "Older chat", "x", 1_000, 0, false, false, false, "", table.ENCRYPTED_OVER_WA_ONE_TO_ONE)
		b.mu.Unlock()
		return nil
	}
	if _, err := b.RestoreHistory(context.Background(), []byte(testPIN)); err != nil {
		t.Fatal(err)
	}
	waitBackfill(t, b)
	seen := map[string]bool{}
	for _, c := range fake.fetches() {
		seen[c.thread] = true
	}
	if calls != 1 || !seen["4242"] || !seen["5555"] {
		t.Fatalf("thread list calls %d, archive threads %v", calls, seen)
	}
	if status := b.Status().HistoryRestore; status.Total != 2 || status.Done != 2 {
		t.Fatalf("status = %#v", status)
	}
}

func TestUnpairForgetsHistoryState(t *testing.T) {
	fake := &fakeHistory{}
	b, _ := newHistoryBackend(t, fake)
	if _, err := b.RestoreHistory(context.Background(), []byte(testPIN)); err != nil {
		t.Fatal(err)
	}
	waitBackfill(t, b)
	// The stand-in E2EE client was never connected; a real one disconnects cleanly.
	b.mu.Lock()
	b.e2eeClient = nil
	b.mu.Unlock()
	if err := b.Unpair(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(b.paths.MessengerHistoryFile()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("history file survived unpair: %v", err)
	}
	if b.confirmedDevice() != nil || b.Status().HistoryRestore != nil {
		t.Fatal("history state survived unpair")
	}
}

// --- Archive conversion ---

func TestArchiveConversionFoldsReactionsEditsAndSkipsNonMessages(t *testing.T) {
	chat := waTypes.NewJID("4242", waTypes.MessengerServer)
	reaction := &waConsumerApplication.ConsumerApplication_Content{Content: &waConsumerApplication.ConsumerApplication_Content_ReactionMessage{
		ReactionMessage: &waConsumerApplication.ConsumerApplication_ReactionMessage{
			Key: &waCommon.MessageKey{ID: proto.String("m1")}, Text: proto.String("👍"),
		},
	}}
	edit := &waConsumerApplication.ConsumerApplication_Content{Content: &waConsumerApplication.ConsumerApplication_Content_EditMessage{
		EditMessage: &waConsumerApplication.ConsumerApplication_EditMessage{
			Key: &waCommon.MessageKey{ID: proto.String("m1")}, Message: &waCommon.MessageText{Text: proto.String("edited text")}, TimestampMS: proto.Int64(2_500),
		},
	}}
	message := archiveText(t, "m1", 4242, 2_000, "original")
	message.Supplementals = map[string]*securehistory.DecryptedProtobuf{
		"r": {Message: archiveBackup(t, "r1", 1, 2_100, reaction), TimestampMS: 2_100},
		"e": {Message: archiveBackup(t, "e1", 4242, 2_500, edit), TimestampMS: 2_500},
	}
	got, err := convertArchiveMessage(chat, 1, message)
	if err != nil || got == nil {
		t.Fatalf("convert = %#v, %v", got, err)
	}
	if got.text != "edited text" || got.sender != 4242 || got.timestampMS != 2_000 || got.reactions[1] != "👍" {
		t.Fatalf("converted = %#v", got)
	}

	// A top-level reaction is not a message of its own.
	if got, err := convertArchiveMessage(chat, 1, &securehistory.DecryptedMessage{
		ID: "x", Message: &securehistory.DecryptedProtobuf{Message: archiveBackup(t, "x", 1, 3_000, reaction), TimestampMS: 3_000},
	}); got != nil || err != nil {
		t.Fatalf("top-level reaction = %#v, %v", got, err)
	}
	// An integrity violation (a reaction aimed at another message) fails the message.
	bad := archiveText(t, "m9", 4242, 2_000, "x")
	bad.Supplementals = map[string]*securehistory.DecryptedProtobuf{"r": {Message: archiveBackup(t, "r1", 1, 2_100, reaction), TimestampMS: 2_100}}
	if _, err := convertArchiveMessage(chat, 1, bad); err == nil {
		t.Fatal("a reaction for a different message was accepted")
	}
	// Admin and event payloads are skipped.
	admin := &securehistory.DecryptedMessage{ID: "a", Message: &securehistory.DecryptedProtobuf{Message: &waArmadilloBackupMessage.BackupMessage{
		Payload: &waArmadilloBackupMessage.BackupMessage_MiTransportAdminMessage{},
	}}}
	if got, err := convertArchiveMessage(chat, 1, admin); got != nil || err != nil {
		t.Fatalf("admin message = %#v, %v", got, err)
	}
}

func TestArchiveMediaUsesAttachmentPathAndStaysSafe(t *testing.T) {
	chat := waTypes.NewJID("4242", waTypes.MessengerServer)
	image := &waConsumerApplication.ConsumerApplication_ImageMessage{Caption: &waCommon.MessageText{Text: proto.String("look")}}
	transport := &waMediaTransport.WAMediaTransport{
		Integral:  &waMediaTransport.WAMediaTransport_Integral{DirectPath: proto.String("/old/path"), MediaKey: make([]byte, 32), FileSHA256: make([]byte, 32), FileEncSHA256: make([]byte, 32)},
		Ancillary: &waMediaTransport.WAMediaTransport_Ancillary{ObjectID: proto.String("obj-1"), Mimetype: proto.String("image/jpeg"), FileLength: proto.Uint64(10)},
	}
	if err := image.Set(&waMediaTransport.ImageTransport{Integral: &waMediaTransport.ImageTransport_Integral{Transport: transport}}); err != nil {
		t.Fatal(err)
	}
	content := &waConsumerApplication.ConsumerApplication_Content{Content: &waConsumerApplication.ConsumerApplication_Content_ImageMessage{ImageMessage: image}}
	build := func(url string) *securehistory.DecryptedMessage {
		return &securehistory.DecryptedMessage{
			ID: "img", Message: &securehistory.DecryptedProtobuf{Message: archiveBackup(t, "img", 4242, 2_000, content), TimestampMS: 2_000},
			Attachments: []securehistory.ArchiveAttachment{{ObjectID: "obj-1", URL: url}},
		}
	}
	got, err := convertArchiveMessage(chat, 1, build("/v/t.fresh/path?x=1"))
	if err != nil || got == nil || got.media == nil {
		t.Fatalf("convert = %#v, %v", got, err)
	}
	if got.text != "look" || !got.media.isImage || got.media.fbTransport.GetDirectPath() != "/v/t.fresh/path?x=1" {
		t.Fatalf("media = %#v", got.media)
	}
	for _, hostile := range []string{"https://evil.example/x", "//evil.example/x", "/ok\r\nHost: x", ""} {
		got, err := convertArchiveMessage(chat, 1, build(hostile))
		if err != nil || got.media.fbTransport.GetDirectPath() != "/old/path" {
			t.Fatalf("hostile path %q was applied: %#v", hostile, got.media.fbTransport.GetDirectPath())
		}
	}

	b := New(zerolog.Nop(), nil, nil)
	b.mu.Lock()
	b.storeArchiveMessageLocked(9, got)
	b.mu.Unlock()
	list := b.messages["9"]
	if len(list) != 1 || len(list[0].Attachments) != 1 || !list[0].Attachments[0].IsImage || b.media[list[0].Attachments[0].Key] == nil {
		t.Fatalf("stored media message = %#v", list)
	}
}

func TestArchiveEphemeralMessageIsWithheld(t *testing.T) {
	chat := waTypes.NewJID("4242", waTypes.MessengerServer)
	app := &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
		Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{Content: textContent("secret")},
	}}
	data, _ := marshalSentFBMessage(app, &waMsgApplication.MessageApplication_Metadata{
		Ephemeral: &waMsgApplication.MessageApplication_Metadata_ChatEphemeralSetting{
			ChatEphemeralSetting: &waMsgApplication.MessageApplication_EphemeralSetting{EphemeralExpiration: proto.Uint32(60)},
		},
	})
	backup := &waArmadilloBackupMessage.BackupMessage{
		Metadata: &waArmadilloBackupCommon.Metadata{SenderID: proto.String("4242"), MessageID: proto.String("eph"), TimestampMS: proto.Int64(1_000)},
		Payload:  &waArmadilloBackupMessage.BackupMessage_EncryptedTransportMessage{EncryptedTransportMessage: data},
	}
	got, err := convertArchiveMessage(chat, 1, &securehistory.DecryptedMessage{ID: "eph", Message: &securehistory.DecryptedProtobuf{Message: backup, TimestampMS: 1_000}})
	if err != nil || got == nil || !got.restricted {
		t.Fatalf("ephemeral archive message = %#v, %v", got, err)
	}
	b := New(zerolog.Nop(), nil, nil)
	b.mu.Lock()
	b.storeArchiveMessageLocked(9, got)
	b.mu.Unlock()
	if list := b.messages["9"]; len(list) != 1 || list[0].Text != lifetimeRestrictedText {
		t.Fatalf("stored = %#v", list)
	}
}

func TestStoringArchiveDoesNotReplaceLiveMessages(t *testing.T) {
	b := New(zerolog.Nop(), nil, nil)
	b.messages["9"] = []wire.Message{{ID: "same", ConversationID: "9", Text: "live copy", Timestamp: messengerTimestamp(1)}}
	b.mu.Lock()
	stored := b.storeArchiveMessageLocked(9, &archiveMessage{id: "same", sender: 5, timestampMS: 1, text: "archive copy"})
	b.mu.Unlock()
	if stored || b.messages["9"][0].Text != "live copy" || len(b.messages["9"]) != 1 {
		t.Fatalf("archive replaced a live message: %#v", b.messages["9"])
	}
}

// --- Upload ---

func uploadFixture(t *testing.T, enabled bool, canUpload bool) (*Backend, *fakeHistory) {
	t.Helper()
	fake := &fakeHistory{canUpload: canUpload}
	b, _ := newHistoryBackend(t, fake)
	cs := appStore.NewConfigStore(b.paths.ConfigFile())
	if enabled {
		if err := cs.SetMessengerSecureHistoryUpload(true); err != nil {
			t.Fatal(err)
		}
	}
	b.SetConfig(cs)
	return b, fake
}

func enroll(t *testing.T, b *Backend) {
	t.Helper()
	if _, err := b.RestoreHistory(context.Background(), []byte(testPIN)); err != nil {
		t.Fatal(err)
	}
	waitBackfill(t, b)
}

func sentFixture() (waTypes.JID, *waConsumerApplication.ConsumerApplication, *waMsgApplication.MessageApplication_Metadata) {
	content := &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
		Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{Content: textContent("hello")},
	}}
	return waTypes.NewJID("4242", waTypes.MessengerServer), content, &waMsgApplication.MessageApplication_Metadata{FrankingKey: make([]byte, 32)}
}

func waitUploads(fake *fakeHistory, want int) int {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		fake.mu.Lock()
		n := len(fake.uploads)
		fake.mu.Unlock()
		if n >= want {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return len(fake.uploads)
}

func TestSecureHistoryUploadIsOffByDefault(t *testing.T) {
	b, fake := uploadFixture(t, false, true)
	enroll(t, b)
	jid, content, meta := sentFixture()
	b.uploadSentMessageToHistory(jid, "1234567890123456", content, meta)
	time.Sleep(100 * time.Millisecond)
	if n := waitUploads(fake, 1); n != 0 {
		t.Fatalf("%d messages were uploaded with the option off", n)
	}
	if (appStore.Config{}).MessengerSecureHistoryUpload {
		t.Fatal("the upload option must default to off")
	}
}

func TestSecureHistoryUploadSendsOnlyWhenEnabledAndEnrolled(t *testing.T) {
	b, fake := uploadFixture(t, true, true)
	jid, content, meta := sentFixture()
	// Enabled but no backup device: nothing is sent.
	b.uploadSentMessageToHistory(jid, "1234567890123456", content, meta)
	time.Sleep(100 * time.Millisecond)
	if n := waitUploads(fake, 1); n != 0 {
		t.Fatalf("uploaded %d messages with no backup device", n)
	}

	enroll(t, b)
	b.uploadSentMessageToHistory(jid, "1234567890123456", content, meta)
	if n := waitUploads(fake, 1); n != 1 {
		t.Fatalf("uploads = %d, want 1", n)
	}
	up := fake.uploads[0]
	if up.BackupID != "backup-1" || up.OtherUserID != 4242 || up.SenderID != 1 || up.MessageID != "1234567890123456" || len(up.FrankingTag) != 32 || len(up.TransportBytes) == 0 || up.TimestampMS != 1234567890123456>>22 {
		t.Fatalf("upload = %#v", up)
	}

	// Group chats, a missing franking key, and an unavailable app ID are skipped.
	group := waTypes.NewJID("999", waTypes.GroupServer)
	b.uploadSentMessageToHistory(group, "1234567890123457", content, meta)
	b.uploadSentMessageToHistory(jid, "1234567890123458", content, &waMsgApplication.MessageApplication_Metadata{})
	b.uploadSentMessageToHistory(jid, "1234567890123459", content, nil)
	time.Sleep(100 * time.Millisecond)
	if n := waitUploads(fake, 2); n != 1 {
		t.Fatalf("uploads = %d, want the one-to-one message only", n)
	}

	fake.canUpload = false
	b.uploadSentMessageToHistory(jid, "1234567890123460", content, meta)
	time.Sleep(100 * time.Millisecond)
	if n := waitUploads(fake, 2); n != 1 {
		t.Fatalf("uploaded without an app ID: %d", n)
	}
}

func TestSecureHistoryUploadFollowsTheLiveSetting(t *testing.T) {
	b, fake := uploadFixture(t, true, true)
	enroll(t, b)
	jid, content, meta := sentFixture()
	if err := b.config.SetMessengerSecureHistoryUpload(false); err != nil {
		t.Fatal(err)
	}
	b.uploadSentMessageToHistory(jid, "1234567890123456", content, meta)
	time.Sleep(100 * time.Millisecond)
	if n := waitUploads(fake, 1); n != 0 {
		t.Fatalf("turning the option off did not stop uploads (%d)", n)
	}
}

func TestMarshalSentFBMessageMatchesWhatsmeowTransport(t *testing.T) {
	_, content, meta := sentFixture()
	data, err := marshalSentFBMessage(content, meta)
	if err != nil {
		t.Fatal(err)
	}
	var app waMsgApplication.MessageApplication
	if err := proto.Unmarshal(data, &app); err != nil {
		t.Fatal(err)
	}
	decoded, err := whatsmeow.DecodeFBApplication(&app)
	if err != nil {
		t.Fatal(err)
	}
	if text, _ := fbContent(&decoded); text != "hello" {
		t.Fatalf("round trip text = %q", text)
	}
	if !proto.Equal(app.GetMetadata(), meta) {
		t.Fatal("metadata changed in transport")
	}
}
