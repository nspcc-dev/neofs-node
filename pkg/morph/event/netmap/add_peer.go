package netmap

import (
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
)

type AddPeer struct {
	node []byte

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements Neo:Morph Event interface.
func (AddPeer) MorphEvent() {}

func (s AddPeer) Node() []byte {
	return s.node
}

// NotaryRequest returns raw notary request if notification
// was received via notary service. Otherwise, returns nil.
func (s AddPeer) NotaryRequest() *payload.P2PNotaryRequest {
	return s.notaryRequest
}
