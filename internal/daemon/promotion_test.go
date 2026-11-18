package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"

	"github.com/onelegdave/omachat/internal/wire"
)

func TestIsShortCodeNumber(t *testing.T) {
	cases := map[string]bool{
		"":                 true,
		"12345":            true,
		"123456":           true,
		"1234567":          false,
		"(555) 010-1234":   false,
		"+1 555 010 1234":  false,
		"+44 7700 900123":  false,
		"AMAZON":           true,
		"Bank-Alert":       true,
		"+1 555 010 1234x": true,
		"555-010":          true,
	}
	for in, want := range cases {
		if got := isShortCodeNumber(in); got != want {
			t.Errorf("isShortCodeNumber(%q) = %v, want %v", in, got, want)
		}
	}
}

func gmConv(typ gmproto.ConversationType, mine bool, group bool, parts ...*gmproto.Participant) *gmproto.Conversation {
	c := &gmproto.Conversation{ConversationID: "c1", Type: typ, IsGroupChat: group, Participants: parts}
	if mine {
		c.LatestMessage = &gmproto.LatestMessage{FromMe: 1}
	}
	return c
}

func person(number, contactID string) *gmproto.Participant {
	return &gmproto.Participant{FormattedNumber: number, ContactID: contactID}
}

var me = &gmproto.Participant{IsMe: true, FormattedNumber: "+15550000000"}

func testDaemon(t *testing.T) *Daemon {
	t.Helper()
	return &Daemon{
		promo:  newPromotionStore(filepath.Join(t.TempDir(), "sent.json")),
		convs:  map[string]wire.Conversation{},
		subs:   map[chan wire.Event]struct{}{},
		status: wire.Status{},
	}
}

func TestConvertConversationKindAndContact(t *testing.T) {
	w := convertConversation(gmConv(gmproto.ConversationType_RCS, false, false, me, person("+1 555 010 1234", "ct1")))
	if w.Kind != "rcs" || !w.Contact || !w.Known {
		t.Fatalf("rcs contact: %+v", w)
	}
	w = convertConversation(gmConv(gmproto.ConversationType_SMS, false, false, me, person("+1 555 010 1234", "")))
	if w.Kind != "sms" || w.Contact || w.Known {
		t.Fatalf("sms unknown: %+v", w)
	}
	w = convertConversation(gmConv(gmproto.ConversationType_UNKNOWN_CONVERSATION_TYPE, false, false, me, person("+1 555 010 1234", "")))
	if w.Kind != "" {
		t.Fatalf("unknown kind = %q", w.Kind)
	}
	// Only the other participant's saved status counts, never the user's own.
	own := &gmproto.Participant{IsMe: true, ContactID: "mine"}
	if w := convertConversation(gmConv(gmproto.ConversationType_SMS, false, false, own, person("+1 555 010 1234", ""))); w.Contact {
		t.Fatalf("own contact id counted: %+v", w)
	}
}

func TestApplyPromotion(t *testing.T) {
	d := testDaemon(t)

	// Replied-to number becomes known and the reply is remembered.
	w := convertConversation(gmConv(gmproto.ConversationType_SMS, true, false, me, person("+1 555 010 1234", "")))
	d.applyPromotion(&w)
	if !w.Known || w.Contact {
		t.Fatalf("replied thread: %+v", w)
	}
	w = convertConversation(gmConv(gmproto.ConversationType_SMS, false, false, me, person("+1 555 010 1234", "")))
	d.applyPromotion(&w)
	if !w.Known {
		t.Fatal("promotion must stick once the preview is no longer mine")
	}

	// Short codes are never promoted, even when the user replied STOP.
	d2 := testDaemon(t)
	w = convertConversation(gmConv(gmproto.ConversationType_SMS, true, false, me, person("48123", "")))
	d2.applyPromotion(&w)
	if w.Known || d2.promo.sent("c1") {
		t.Fatalf("short code promoted: %+v", w)
	}
	w = convertConversation(gmConv(gmproto.ConversationType_SMS, true, false, me, person("ALERTS", "")))
	d2.applyPromotion(&w)
	if w.Known {
		t.Fatalf("alphanumeric sender promoted: %+v", w)
	}

	// Never replied: stays in Others.
	w = convertConversation(gmConv(gmproto.ConversationType_SMS, false, false, me, person("+1 555 010 9999", "")))
	d3 := testDaemon(t)
	d3.applyPromotion(&w)
	if w.Known {
		t.Fatalf("unreplied thread known: %+v", w)
	}
}

func TestPromotionStorePersistsAtomicallyAndPrivately(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sent.json")
	s := newPromotionStore(path)
	if !s.markSent("a") || s.markSent("a") {
		t.Fatal("markSent change reporting")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v", info.Mode().Perm())
	}
	if !newPromotionStore(path).sent("a") {
		t.Fatal("not persisted")
	}
	if matches, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".omachat-*.tmp")); len(matches) != 0 {
		t.Fatalf("leftover temp files: %v", matches)
	}
	s.reset()
	if _, err := os.Stat(path); !os.IsNotExist(err) || s.sent("a") {
		t.Fatal("reset did not clear")
	}
}

func TestPromotionClaimRechecksOncePerDay(t *testing.T) {
	s := newPromotionStore(filepath.Join(t.TempDir(), "sent.json"))
	now := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return now }
	if !s.claim("a") || s.claim("a") {
		t.Fatal("inflight claim must be exclusive")
	}
	s.release("a")
	s.recordChecked("a", false)
	if s.claim("a") {
		t.Fatal("rechecked within a day")
	}
	now = now.Add(recheckAfter + time.Minute)
	if !s.claim("a") {
		t.Fatal("not rechecked after a day")
	}
	s.release("a")
	s.recordChecked("a", true)
	now = now.Add(100 * recheckAfter)
	if s.claim("a") {
		t.Fatal("promoted thread must never be refetched")
	}
}

func TestCheckPromotionsPromotesOnSentMessage(t *testing.T) {
	d := testDaemon(t)
	mk := func(id string, number string) wire.Conversation {
		return wire.Conversation{ID: id, Participants: []wire.Participant{{IsMe: true}, {Number: number}}}
	}
	convs := []wire.Conversation{mk("sent", "+1 555 010 1111"), mk("quiet", "+1 555 010 2222"), mk("short", "48123")}
	for _, c := range convs {
		d.convs[c.ID] = c
	}
	ch := make(chan wire.Event, 8)
	d.subs[ch] = struct{}{}
	d.sessionCtx = context.Background()

	var fetched []string
	fetch := func(ctx context.Context, id string, count int64) ([]*gmproto.Message, error) {
		fetched = append(fetched, id)
		if count != promotionFetchCount {
			t.Errorf("count = %d", count)
		}
		in := &gmproto.Message{MessageStatus: &gmproto.MessageStatus{Status: gmproto.MessageStatusType_INCOMING_COMPLETE}}
		out := &gmproto.Message{MessageStatus: &gmproto.MessageStatus{Status: gmproto.MessageStatusType_OUTGOING_DELIVERED}}
		if id == "sent" {
			return []*gmproto.Message{in, out}, nil
		}
		return []*gmproto.Message{in}, nil
	}
	d.checkPromotions(context.Background(), convs, fetch)

	if len(fetched) != 2 {
		t.Fatalf("fetched %v (short codes must be skipped)", fetched)
	}
	if !d.convs["sent"].Known || d.convs["quiet"].Known || d.convs["short"].Known {
		t.Fatalf("known flags: %+v", d.convs)
	}
	select {
	case evt := <-ch:
		if evt.Event != wire.EventConversation || evt.Data.(wire.Conversation).ID != "sent" {
			t.Fatalf("event = %+v", evt)
		}
	default:
		t.Fatal("no conversation event for promoted thread")
	}

	// Second pass: nothing refetched within the day, and the sent cache hits.
	fetched = nil
	d.checkPromotions(context.Background(), convs, fetch)
	if len(fetched) != 0 {
		t.Fatalf("refetched %v", fetched)
	}
}

func TestCheckPromotionsStopsOnFetchError(t *testing.T) {
	d := testDaemon(t)
	convs := []wire.Conversation{
		{ID: "a", Participants: []wire.Participant{{Number: "+1 555 010 1111"}}},
		{ID: "b", Participants: []wire.Participant{{Number: "+1 555 010 2222"}}},
	}
	calls := 0
	d.checkPromotions(context.Background(), convs, func(context.Context, string, int64) ([]*gmproto.Message, error) {
		calls++
		return nil, errors.New("offline")
	})
	if calls != 1 {
		t.Fatalf("calls = %d", calls)
	}
	if !d.promo.claim("a") {
		t.Fatal("a failed fetch must not be recorded as checked")
	}
}

func TestServiceDefaults(t *testing.T) {
	c := wire.Conversation{ID: "1"}.WithServiceDefaults(wire.NetworkMessenger)
	if c.Kind != "messenger" || !c.Contact || !c.Known {
		t.Fatalf("%+v", c)
	}
	evt := decorateServiceEvent(wire.Event{Event: wire.EventConversation, Network: wire.NetworkTelegram, Data: wire.Conversation{ID: "2"}})
	if got := evt.Data.(wire.Conversation); !got.Known || got.Kind != "telegram" {
		t.Fatalf("%+v", got)
	}
	evt = decorateServiceEvent(wire.Event{Event: wire.EventConversation, Network: wire.NetworkGMessages, Data: wire.Conversation{ID: "3"}})
	if evt.Data.(wire.Conversation).Known {
		t.Fatal("gmessages must not be defaulted")
	}
}
