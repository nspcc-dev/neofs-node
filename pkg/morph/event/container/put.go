package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
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

// PutSuccess structures notification event of successful container creation
// thrown by Container contract.
type PutSuccess struct {
	// Identifier of the newly created container.
	ID cid.ID
}

// MorphEvent implements Neo:Morph Event interface.
func (PutSuccess) MorphEvent() {}

// ParsePutSuccess decodes notification event thrown by Container contract into
// PutSuccess and returns it as event.Event.
func ParsePutSuccess(e *state.ContainedNotificationEvent) (event.Event, error) {
	items, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("parse stack array from raw notification event: %w", err)
	}

	const expectedItemNumPutSuccess = 2

	if ln := len(items); ln != expectedItemNumPutSuccess {
		return nil, event.WrongNumberOfParameters(expectedItemNumPutSuccess, ln)
	}

	binID, err := client.BytesFromStackItem(items[0])
	if err != nil {
		return nil, fmt.Errorf("parse container ID item: %w", err)
	}

	_, err = client.BytesFromStackItem(items[1])
	if err != nil {
		return nil, fmt.Errorf("parse public key item: %w", err)
	}

	var res PutSuccess

	err = res.ID.Decode(binID)
	if err != nil {
		return nil, fmt.Errorf("decode container ID: %w", err)
	}

	return res, nil
}
