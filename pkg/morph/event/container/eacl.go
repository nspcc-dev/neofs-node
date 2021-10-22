package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result/subscriptions"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// SetEACL represents structure of notification about
// modified eACL table coming from NeoFS Container contract.
type SetEACL struct {
	table     []byte
	signature []byte
	publicKey []byte
	token     []byte

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements Neo:Morph Event interface.
func (SetEACL) MorphEvent() {}

// Table returns returns eACL table in a binary NeoFS API format.
func (x SetEACL) Table() []byte {
	return x.table
}

// Signature returns signature of the binary table.
func (x SetEACL) Signature() []byte {
	return x.signature
}

// PublicKey returns public keys of container
// owner in a binary format.
func (x SetEACL) PublicKey() []byte {
	return x.publicKey
}

// SessionToken returns binary token of the session
// within which the eACL was set.
func (x SetEACL) SessionToken() []byte {
	return x.token
}

// NotaryRequest returns raw notary request if notification
// was received via notary service. Otherwise, returns nil.
func (x SetEACL) NotaryRequest() *payload.P2PNotaryRequest {
	return x.notaryRequest
}

const expectedItemNumEACL = 4

// ParseSetEACL parses SetEACL notification event from list of stack items.
//
// Expects 4 stack items.
func ParseSetEACL(e *subscriptions.NotificationEvent) (event.Event, error) {
	var (
		ev  SetEACL
		err error
	)

	params, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("could not parse stack items from notify event: %w", err)
	}

	if ln := len(params); ln != expectedItemNumEACL {
		return nil, event.WrongNumberOfParameters(expectedItemNumEACL, ln)
	}

	// parse table
	ev.table, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, fmt.Errorf("could not parse binary table: %w", err)
	}

	// parse signature
	ev.signature, err = client.BytesFromStackItem(params[1])
	if err != nil {
		return nil, fmt.Errorf("could not parse table signature: %w", err)
	}

	// parse public key
	ev.publicKey, err = client.BytesFromStackItem(params[2])
	if err != nil {
		return nil, fmt.Errorf("could not parse binary public key: %w", err)
	}

	// parse session token
	ev.token, err = client.BytesFromStackItem(params[3])
	if err != nil {
		return nil, fmt.Errorf("could not get session token: %w", err)
	}

	return ev, nil
}
