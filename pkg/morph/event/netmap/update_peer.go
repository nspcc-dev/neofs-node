package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neofs-contract/contracts/netmap/nodestate"
)

type UpdatePeer struct {
	publicKey *keys.PublicKey

	state nodestate.Type

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements Neo:Morph Event interface.
func (UpdatePeer) MorphEvent() {}

// Online returns true if node's state is requested to be switched
// to "online".
func (s UpdatePeer) Online() bool {
	return s.state == nodestate.Online
}

// Maintenance returns true if node's state is requested to be switched
// to "maintenance".
func (s UpdatePeer) Maintenance() bool {
	return s.state == nodestate.Maintenance
}

func (s UpdatePeer) PublicKey() *keys.PublicKey {
	return s.publicKey
}

// NotaryRequest returns raw notary request if notification
// was received via notary service. Otherwise, returns nil.
func (s UpdatePeer) NotaryRequest() *payload.P2PNotaryRequest {
	return s.notaryRequest
}

func (s *UpdatePeer) decodeState(state int64) error {
	switch s.state = nodestate.Type(state); s.state {
	default:
		return fmt.Errorf("unsupported node state %d", state)
	case
		nodestate.Offline,
		nodestate.Online,
		nodestate.Maintenance:
		return nil
	}
}
