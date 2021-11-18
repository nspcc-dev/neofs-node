package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result/subscriptions"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// Delete structure of container.Delete notification from morph chain.
type Delete struct {
	containerID []byte
	signature   []byte
	token       []byte

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements Neo:Morph Event interface.
func (Delete) MorphEvent() {}

// ContainerID is a marshalled container structure, defined in API.
func (d Delete) ContainerID() []byte { return d.containerID }

// Signature of marshalled container by container owner.
func (d Delete) Signature() []byte { return d.signature }

// SessionToken returns binary token of the session
// within which the eACL was set.
func (d Delete) SessionToken() []byte {
	return d.token
}

// NotaryRequest returns raw notary request if notification
// was received via notary service. Otherwise, returns nil.
func (d Delete) NotaryRequest() *payload.P2PNotaryRequest {
	return d.notaryRequest
}

const expectedItemNumDelete = 3

// ParseDelete from notification into container event structure.
//
// Expects 3 stack items.
func ParseDelete(e *subscriptions.NotificationEvent) (event.Event, error) {
	var (
		ev  Delete
		err error
	)

	params, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("could not parse stack items from notify event: %w", err)
	}

	if ln := len(params); ln != expectedItemNumDelete {
		return nil, event.WrongNumberOfParameters(expectedItemNumDelete, ln)
	}

	// parse container
	ev.containerID, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, fmt.Errorf("could not get container: %w", err)
	}

	// parse signature
	ev.signature, err = client.BytesFromStackItem(params[1])
	if err != nil {
		return nil, fmt.Errorf("could not get signature: %w", err)
	}

	// parse session token
	ev.token, err = client.BytesFromStackItem(params[2])
	if err != nil {
		return nil, fmt.Errorf("could not get session token: %w", err)
	}

	return ev, nil
}
