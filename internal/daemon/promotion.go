package daemon

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"
	"unicode"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"

	"github.com/onelegdave/omachat/internal/store"
	"github.com/onelegdave/omachat/internal/wire"
)

// recheckAfter bounds how often a thread the user has not replied in is
// fetched again to see whether that changed.
const recheckAfter = 24 * time.Hour

// promotionFetchCount is how many recent messages are inspected for a reply.
const promotionFetchCount = 25

// isShortCodeNumber reports whether a sender looks like a business: a short
// code (six digits or fewer once punctuation is dropped) or an alphanumeric
// sender ID. People are not reachable on either.
func isShortCodeNumber(number string) bool {
	digits := 0
	for _, r := range number {
		switch {
		case unicode.IsLetter(r):
			return true
		case unicode.IsDigit(r):
			digits++
		}
	}
	return digits <= 6
}

// isShortCodeConversation reports whether a one-to-one thread is with a short
// code. Groups are never short codes.
func isShortCodeConversation(parts []wire.Participant, group bool) bool {
	if group {
		return false
	}
	for _, p := range parts {
		if !p.IsMe {
			return isShortCodeNumber(p.Number)
		}
	}
	return false
}

// hasSavedContact reports whether any other participant is in the phone's contacts.
func hasSavedContact(conv *gmproto.Conversation) bool {
	for _, p := range conv.GetParticipants() {
		if !p.GetIsMe() && p.GetContactID() != "" {
			return true
		}
	}
	return false
}

func conversationKind(t gmproto.ConversationType) string {
	switch t {
	case gmproto.ConversationType_SMS:
		return wire.KindSMS
	case gmproto.ConversationType_RCS:
		return wire.KindRCS
	}
	return ""
}

type promotionEntry struct {
	Sent    bool  `json:"sent,omitempty"`
	Checked int64 `json:"checked,omitempty"` // unix seconds of the last negative check
}

// promotionStore remembers, per conversation, whether the user has sent a
// message in it. Once true it stays true. It is persisted so a restart does
// not refetch every thread.
type promotionStore struct {
	path string
	now  func() time.Time

	mu       sync.Mutex
	loaded   bool
	entries  map[string]promotionEntry
	inflight map[string]bool
}

func newPromotionStore(path string) *promotionStore {
	return &promotionStore{path: path, now: time.Now, entries: map[string]promotionEntry{}, inflight: map[string]bool{}}
}

func (s *promotionStore) loadLocked() {
	if s.loaded {
		return
	}
	s.loaded = true
	if s.path == "" {
		return
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	var entries map[string]promotionEntry
	if json.Unmarshal(data, &entries) == nil && entries != nil {
		s.entries = entries
	}
}

func (s *promotionStore) saveLocked() {
	if s.path == "" {
		return
	}
	data, err := json.Marshal(s.entries)
	if err != nil {
		return
	}
	_ = store.WritePrivateFile(s.path, data)
}

// sent reports whether the user is known to have replied in the thread.
func (s *promotionStore) sent(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadLocked()
	return s.entries[id].Sent
}

// markSent records a reply. It returns true when this changed the record.
func (s *promotionStore) markSent(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadLocked()
	if s.entries[id].Sent {
		return false
	}
	s.entries[id] = promotionEntry{Sent: true}
	s.saveLocked()
	return true
}

// recordChecked stores the result of a message fetch.
func (s *promotionStore) recordChecked(id string, sent bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadLocked()
	if s.entries[id].Sent {
		return
	}
	s.entries[id] = promotionEntry{Sent: sent, Checked: s.now().Unix()}
	s.saveLocked()
}

// claim reserves a thread for checking. It refuses threads already known to
// be promoted, checked within recheckAfter, or being checked right now.
func (s *promotionStore) claim(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadLocked()
	e := s.entries[id]
	if e.Sent || s.inflight[id] {
		return false
	}
	if e.Checked != 0 && s.now().Sub(time.Unix(e.Checked, 0)) < recheckAfter {
		return false
	}
	s.inflight[id] = true
	return true
}

func (s *promotionStore) release(id string) {
	s.mu.Lock()
	delete(s.inflight, id)
	s.mu.Unlock()
}

func (s *promotionStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loaded = true
	s.entries = map[string]promotionEntry{}
	s.inflight = map[string]bool{}
	if s.path != "" {
		_ = os.Remove(s.path)
	}
}

// applyPromotion finalizes Known for a converted conversation: a saved
// contact, or a thread with a person the user has replied to. A latest message
// from the user counts as a reply and is remembered.
func (d *Daemon) applyPromotion(w *wire.Conversation) {
	w.Known = w.Contact
	if w.Known || d.promo == nil || isShortCodeConversation(w.Participants, w.IsGroup) {
		return
	}
	if w.PreviewMine {
		d.promo.markSent(w.ID)
	}
	w.Known = d.promo.sent(w.ID)
}

// messagesFetcher returns the most recent messages of a conversation.
type messagesFetcher func(ctx context.Context, conversationID string, count int64) ([]*gmproto.Message, error)

func anyFromMe(msgs []*gmproto.Message) bool {
	for _, m := range msgs {
		if isOutgoingStatus(m.GetMessageStatus().GetStatus()) {
			return true
		}
	}
	return false
}

// checkPromotions looks, in the background, for a reply from the user in each
// unknown thread that is not a short code, and republishes the ones that turn
// out to be promoted. It stops at the first fetch error and retries on the
// next sync.
func (d *Daemon) checkPromotions(ctx context.Context, convs []wire.Conversation, fetch messagesFetcher) {
	for _, conv := range convs {
		if ctx.Err() != nil {
			return
		}
		if conv.Known || isShortCodeConversation(conv.Participants, conv.IsGroup) || !d.promo.claim(conv.ID) {
			continue
		}
		msgs, err := fetch(ctx, conv.ID, promotionFetchCount)
		d.promo.release(conv.ID)
		if err != nil {
			return
		}
		sent := anyFromMe(msgs)
		d.promo.recordChecked(conv.ID, sent)
		if sent {
			d.republishKnown(ctx, conv.ID)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// republishKnown marks a cached conversation known and tells the panel.
func (d *Daemon) republishKnown(ctx context.Context, id string) {
	d.inSession(ctx, func() {
		d.mu.Lock()
		w, ok := d.convs[id]
		if ok && !w.Known {
			w.Known = true
			d.convs[id] = w
		} else {
			ok = false
		}
		d.mu.Unlock()
		if ok {
			d.publish(wire.EventConversation, w)
		}
	})
}

// startPromotionChecks launches the background check for a freshly synced list.
func (d *Daemon) startPromotionChecks(convs []wire.Conversation) {
	if d.promo == nil {
		return
	}
	var pending []wire.Conversation
	for _, c := range convs {
		if !c.Known && !isShortCodeConversation(c.Participants, c.IsGroup) {
			pending = append(pending, c)
		}
	}
	if len(pending) == 0 {
		return
	}
	ctx := d.sessionContext()
	go d.checkPromotions(ctx, pending, func(ctx context.Context, id string, count int64) ([]*gmproto.Message, error) {
		// A sync can finish a moment before the state flips to connected.
		c, err := d.requireClient()
		for i := 0; err != nil && i < 10 && waitContext(ctx, time.Second); i++ {
			c, err = d.requireClient()
		}
		if err != nil {
			return nil, err
		}
		resp, err := withAuthRetry(ctx, d, func() (*gmproto.ListMessagesResponse, error) {
			return c.FetchMessages(ctx, id, count, nil)
		})
		if err != nil {
			return nil, err
		}
		return resp.GetMessages(), nil
	})
}
