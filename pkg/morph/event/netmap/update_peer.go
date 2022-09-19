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

// TODO: enum can become redundant after neofs-contract#270
const (
	_ int8 = iota
	stateOnline
)

type UpdatePeer struct {
	publicKey *keys.PublicKey

	state int8 // state enum value

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements Neo:Morph Event interface.
func (UpdatePeer) MorphEvent() {}

func (s UpdatePeer) Online() bool {
	return s.state == stateOnline
}

func (s UpdatePeer) PublicKey() *keys.PublicKey {
	return s.publicKey
}

// NotaryRequest returns raw notary request if notification
// was received via notary service. Otherwise, returns nil.
func (s UpdatePeer) NotaryRequest() *payload.P2PNotaryRequest {
	return s.notaryRequest
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

	switch st {
	default:
		return nil, fmt.Errorf("unsupported node state %d", st)
	case int64(netmap.OfflineState):
	case int64(netmap.OnlineState):
		ev.state = stateOnline
	}

	return ev, nil
}
