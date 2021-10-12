package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// Put structure of container.Put notification from morph chain.
type Put struct {
	rawContainer []byte
	signature    []byte
	publicKey    []byte
	token        []byte

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

const expectedItemNumPut = 4

// MorphEvent implements Neo:Morph Event interface.
func (Put) MorphEvent() {}

// Container is a marshalled container structure, defined in API.
func (p Put) Container() []byte { return p.rawContainer }

// Signature of marshalled container by container owner.
func (p Put) Signature() []byte { return p.signature }

// PublicKey of container owner.
func (p Put) PublicKey() []byte { return p.publicKey }

// SessionToken returns binary token of the session
// within which the container was created.
func (p Put) SessionToken() []byte {
	return p.token
}

// NotaryRequest returns raw notary request if notification
// was received via notary service. Otherwise, returns nil.
func (p Put) NotaryRequest() *payload.P2PNotaryRequest {
	return p.notaryRequest
}

// PutNamed represents notification event spawned by PutNamed method from Container contract of NeoFS Morph chain.
type PutNamed struct {
	Put

	name, zone string
}

// Name returns "name" arg of contract call.
func (x PutNamed) Name() string {
	return x.name
}

// Zone returns "zone" arg of contract call.
func (x PutNamed) Zone() string {
	return x.zone
}

// ParsePut from notification into container event structure.
func ParsePut(params []stackitem.Item) (event.Event, error) {
	var (
		ev  Put
		err error
	)

	if ln := len(params); ln != expectedItemNumPut {
		return nil, event.WrongNumberOfParameters(expectedItemNumPut, ln)
	}

	// parse container
	ev.rawContainer, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, fmt.Errorf("could not get container: %w", err)
	}

	// parse signature
	ev.signature, err = client.BytesFromStackItem(params[1])
	if err != nil {
		return nil, fmt.Errorf("could not get signature: %w", err)
	}

	// parse public key
	ev.publicKey, err = client.BytesFromStackItem(params[2])
	if err != nil {
		return nil, fmt.Errorf("could not get public key: %w", err)
	}

	// parse session token
	ev.token, err = client.BytesFromStackItem(params[3])
	if err != nil {
		return nil, fmt.Errorf("could not get sesison token: %w", err)
	}

	return ev, nil
}
