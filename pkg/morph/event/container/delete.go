package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
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

// DeleteSuccess structures notification event of successful container removal
// thrown by Container contract.
type DeleteSuccess struct {
	// Identifier of the removed container.
	ID cid.ID
}

// MorphEvent implements Neo:Morph Event interface.
func (DeleteSuccess) MorphEvent() {}

// ParseDeleteSuccess decodes notification event thrown by Container contract into
// DeleteSuccess and returns it as event.Event.
func ParseDeleteSuccess(e *state.ContainedNotificationEvent) (event.Event, error) {
	items, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("parse stack array from raw notification event: %w", err)
	}

	const expectedItemNumDeleteSuccess = 1

	if ln := len(items); ln != expectedItemNumDeleteSuccess {
		return nil, event.WrongNumberOfParameters(expectedItemNumDeleteSuccess, ln)
	}

	binID, err := client.BytesFromStackItem(items[0])
	if err != nil {
		return nil, fmt.Errorf("parse container ID item: %w", err)
	}

	var res DeleteSuccess

	err = res.ID.Decode(binID)
	if err != nil {
		return nil, fmt.Errorf("decode container ID: %w", err)
	}

	return res, nil
}
