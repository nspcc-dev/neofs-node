package netmap

import (
	"crypto/elliptic"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neofs-contract/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

type UpdatePeer struct {
	publicKey *keys.PublicKey

	state netmap.NodeState

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements Neo:Morph Event interface.
func (UpdatePeer) MorphEvent() {}

// Online returns true if node's state is requested to be switched
// to "online".
func (s UpdatePeer) Online() bool {
	return s.state == netmap.NodeStateOnline
}

// Maintenance returns true if node's state is requested to be switched
// to "maintenance".
func (s UpdatePeer) Maintenance() bool {
	return s.state == netmap.NodeStateMaintenance
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
	switch s.state = netmap.NodeState(state); s.state {
	default:
		return fmt.Errorf("unsupported node state %d", state)
	case
		netmap.NodeStateOffline,
		netmap.NodeStateOnline,
		netmap.NodeStateMaintenance:
		return nil
	}
}

const expectedItemNumUpdatePeer = 2

func ParseUpdatePeer(e *state.ContainedNotificationEvent) (event.Event, error) {
	var (
		ev  UpdatePeer
		err error
	)

	params, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("could not parse stack items from notify event: %w", err)
	}

	if ln := len(params); ln != expectedItemNumUpdatePeer {
		return nil, event.WrongNumberOfParameters(expectedItemNumUpdatePeer, ln)
	}

	// parse public key
	key, err := client.BytesFromStackItem(params[1])
	if err != nil {
		return nil, fmt.Errorf("could not get public key: %w", err)
	}

	ev.publicKey, err = keys.NewPublicKeyFromBytes(key, elliptic.P256())
	if err != nil {
		return nil, fmt.Errorf("could not parse public key: %w", err)
	}

	// parse node status
	st, err := client.IntFromStackItem(params[0])
	if err != nil {
		return nil, fmt.Errorf("could not get node status: %w", err)
	}

	err = ev.decodeState(st)
	if err != nil {
		return nil, err
	}

	return ev, nil
}
