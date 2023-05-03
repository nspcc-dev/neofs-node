package reputation

import (
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neofs-sdk-go/reputation"
)

// Put structure of reputation.reputationPut notification from
// morph chain.
type Put struct {
	epoch  uint64
	peerID reputation.PeerID
	value  reputation.GlobalTrust

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

const peerIDLength = 33 // compressed public key

// MorphEvent implements Neo:Morph Event interface.
func (Put) MorphEvent() {}

// Epoch returns epoch value of reputation data.
func (p Put) Epoch() uint64 {
	return p.epoch
}

// PeerID returns peer id of reputation data.
func (p Put) PeerID() reputation.PeerID {
	return p.peerID
}

// Value returns reputation structure.
func (p Put) Value() reputation.GlobalTrust {
	return p.value
}

// NotaryRequest returns raw notary request if notification
// was received via notary service. Otherwise, returns nil.
func (p Put) NotaryRequest() *payload.P2PNotaryRequest {
	return p.notaryRequest
}
