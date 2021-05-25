package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// SetEACL represents structure of notification about
// modified eACL table coming from NeoFS Container contract.
type SetEACL struct {
	table []byte

	signature []byte

	publicKey []byte

	token []byte
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

// Session token returns binary token of the session
// within which the eACL was set.
func (x SetEACL) SessionToken() []byte {
	return x.token
}

// ParseSetEACL parses SetEACL notification event from list of stack items.
//
// Expects 4 stack items.
func ParseSetEACL(items []stackitem.Item) (event.Event, error) {
	var (
		ev  SetEACL
		err error
	)

	const expectedItemNumEACL = 4

	if ln := len(items); ln != expectedItemNumEACL {
		return nil, event.WrongNumberOfParameters(expectedItemNumEACL, ln)
	}

	// parse table
	ev.table, err = client.BytesFromStackItem(items[0])
	if err != nil {
		return nil, fmt.Errorf("could not parse binary table: %w", err)
	}

	// parse signature
	ev.signature, err = client.BytesFromStackItem(items[1])
	if err != nil {
		return nil, fmt.Errorf("could not parse table signature: %w", err)
	}

	// parse public key
	ev.publicKey, err = client.BytesFromStackItem(items[2])
	if err != nil {
		return nil, fmt.Errorf("could not parse binary public key: %w", err)
	}

	// parse session token
	ev.token, err = client.BytesFromStackItem(items[3])
	if err != nil {
		return nil, fmt.Errorf("could not get sesison token: %w", err)
	}

	return ev, nil
}
