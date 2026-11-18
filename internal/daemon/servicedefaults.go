package daemon

import "github.com/onelegdave/omachat/internal/wire"

// hasServiceDefaults reports whether a service has no address-book signal, so
// every conversation counts as a known person.
func hasServiceDefaults(network string) bool {
	switch network {
	case wire.NetworkMessenger, wire.NetworkTelegram, wire.NetworkSignal:
		return true
	}
	return false
}

// decorateServiceEvent fills Kind, Contact and Known on conversation events of
// services that do not compute them themselves.
func decorateServiceEvent(evt wire.Event) wire.Event {
	if evt.Event != wire.EventConversation || !hasServiceDefaults(evt.Network) {
		return evt
	}
	switch c := evt.Data.(type) {
	case wire.Conversation:
		evt.Data = c.WithServiceDefaults(evt.Network)
	case *wire.Conversation:
		if c != nil {
			v := c.WithServiceDefaults(evt.Network)
			evt.Data = &v
		}
	}
	return evt
}

// decorateServiceList is decorateServiceEvent for a conversations response.
func decorateServiceList(network string, list []wire.Conversation) []wire.Conversation {
	if !hasServiceDefaults(network) {
		return list
	}
	out := make([]wire.Conversation, len(list))
	for i, c := range list {
		out[i] = c.WithServiceDefaults(network)
	}
	return out
}
