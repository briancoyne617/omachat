package main

import (
	"encoding/json"
	"testing"

	"github.com/onelegdave/omachat/internal/wire"
)

func TestPairRequestIdentifiesAsSafeClient(t *testing.T) {
	req := pairRequest(map[string]string{"SID": "x"})
	if req.Client != wire.SafeClient || req.Method != wire.MethodGaiaPairing || req.ID != "pair" {
		t.Fatalf("unexpected pairing request: %+v", req)
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var frame map[string]any
	if err := json.Unmarshal(encoded, &frame); err != nil {
		t.Fatal(err)
	}
	if frame["client"] != wire.SafeClient {
		t.Fatalf("the daemon rejects a request without the client marker: %s", encoded)
	}
}
