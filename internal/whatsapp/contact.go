package whatsapp

import (
	"context"
	"time"

	"go.mau.fi/whatsmeow/types"

	waStore "go.mau.fi/whatsmeow/store"

	"github.com/onelegdave/omachat/internal/wire"
)

// contactCacheTTL bounds how stale a saved-contact answer may be. The
// address book changes rarely and lookups can reach SQLite.
const contactCacheTTL = 2 * time.Minute

type contactAnswer struct {
	saved   bool
	expires time.Time
}

// conversationContact reports whether a chat is with someone in the phone's
// address book. Groups count as contact chats: member lists are not cheaply
// available, and a group the user is in is not an unsolicited sender.
// Channels, bots and other non-person chats are not contacts.
func conversationContact(ctx context.Context, device *waStore.Device, id string, group bool) bool {
	jid, err := types.ParseJID(id)
	if err != nil {
		return false
	}
	if group || jid.Server == types.GroupServer {
		return true
	}
	if jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer {
		return false
	}
	return lookupSavedContactName(ctx, device, jid) != ""
}

// decorateLocked fills Kind, Contact and Known. The caller holds b.mu, for
// reading at least.
func (b *Backend) decorateLocked(c wire.Conversation) wire.Conversation {
	c.Kind = wire.KindWhatsApp
	c.Contact = b.savedContactLocked(c)
	c.Known = c.Contact
	return c
}

func (b *Backend) savedContactLocked(c wire.Conversation) bool {
	now := time.Now()
	b.contactMu.Lock()
	if a, ok := b.contactCache[c.ID]; ok && now.Before(a.expires) {
		b.contactMu.Unlock()
		return a.saved
	}
	b.contactMu.Unlock()
	ctx := b.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	saved := conversationContact(ctx, b.device, c.ID, c.IsGroup)
	if b.device == nil || b.device.Contacts == nil {
		return saved // not synced yet; do not remember the miss
	}
	b.contactMu.Lock()
	if b.contactCache == nil {
		b.contactCache = make(map[string]contactAnswer)
	}
	if len(b.contactCache) > 4096 {
		clear(b.contactCache)
	}
	b.contactCache[c.ID] = contactAnswer{saved: saved, expires: now.Add(contactCacheTTL)}
	b.contactMu.Unlock()
	return saved
}
