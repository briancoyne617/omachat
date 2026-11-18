package whatsapp

import (
	"context"
	"testing"

	"go.mau.fi/whatsmeow/types"

	waStore "go.mau.fi/whatsmeow/store"

	"github.com/onelegdave/omachat/internal/wire"
)

type fakeContacts struct {
	waStore.ContactStore
	info map[types.JID]types.ContactInfo
}

func (f fakeContacts) GetContact(_ context.Context, jid types.JID) (types.ContactInfo, error) {
	return f.info[jid], nil
}

func TestConversationContact(t *testing.T) {
	saved := types.NewJID("15550101111", types.DefaultUserServer)
	pushOnly := types.NewJID("15550102222", types.DefaultUserServer)
	firstOnly := types.NewJID("15550103333", types.DefaultUserServer)
	dev := &waStore.Device{Contacts: fakeContacts{info: map[types.JID]types.ContactInfo{
		saved:     {Found: true, FullName: "Ada Lovelace"},
		pushOnly:  {Found: true, PushName: "stranger"},
		firstOnly: {Found: true, FirstName: "Bea"},
	}}}
	ctx := context.Background()
	cases := []struct {
		id    string
		group bool
		want  bool
	}{
		{saved.String(), false, true},
		{firstOnly.String(), false, true},
		{pushOnly.String(), false, false},
		{"15550109999@s.whatsapp.net", false, false},
		{"120363000000000000@g.us", true, true},
		{"120363000000000000@newsletter", false, false},
		{"not a jid", false, false},
	}
	for _, c := range cases {
		if got := conversationContact(ctx, dev, c.id, c.group); got != c.want {
			t.Errorf("conversationContact(%q, group=%v) = %v, want %v", c.id, c.group, got, c.want)
		}
	}
	if conversationContact(ctx, nil, saved.String(), false) {
		t.Error("no device must not report a contact")
	}
}

func TestDecorateLockedSetsKindContactKnown(t *testing.T) {
	saved := types.NewJID("15550101111", types.DefaultUserServer)
	b := &Backend{device: &waStore.Device{Contacts: fakeContacts{info: map[types.JID]types.ContactInfo{
		saved: {Found: true, FullName: "Ada"},
	}}}}
	got := b.decorateLocked(wire.Conversation{ID: saved.String()})
	if got.Kind != "whatsapp" || !got.Contact || !got.Known {
		t.Fatalf("%+v", got)
	}
	got = b.decorateLocked(wire.Conversation{ID: "15550109999@s.whatsapp.net"})
	if got.Kind != "whatsapp" || got.Contact || got.Known {
		t.Fatalf("%+v", got)
	}
}
