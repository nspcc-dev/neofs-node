package container

import (
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

const (
	// SetAttributeNotaryEvent is method name for setAttribute operations in `Container` contract.
	SetAttributeNotaryEvent = "setAttribute"
)

// SetAttribute represents structure of notification about modified container attributes
// coming from NeoFS Container contract.
type SetAttribute struct {
	CID   []byte
	Name  []byte
	Value []byte
	Token []byte

	// For notary notifications only.
	// Contains raw transactions of notary request.
	NotaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements [event.Event].
func (r SetAttribute) MorphEvent() {}

// ParseSetAttribute from NotaryEvent into container event structure.
func ParseSetAttribute(ne event.NotaryEvent) (event.Event, error) {
	const expectedItemNumAnnounceLoad = 4
	args, err := getArgsFromEvent(ne, expectedItemNumAnnounceLoad)
	if err != nil {
		return nil, err
	}
	var ev SetAttribute

	ev.Token, err = getValueFromArg(args, 0, "session token", stackitem.ByteArrayT, event.BytesFromOpcode)
	if err != nil {
		return nil, err
	}
	ev.Value, err = getValueFromArg(args, 1, "attribute value", stackitem.ByteArrayT, event.BytesFromOpcode)
	if err != nil {
		return nil, err
	}
	ev.Name, err = getValueFromArg(args, 2, "attribute name", stackitem.ByteArrayT, event.BytesFromOpcode)
	if err != nil {
		return nil, err
	}
	ev.CID, err = getValueFromArg(args, 3, "container ID", stackitem.ByteArrayT, event.BytesFromOpcode)
	if err != nil {
		return nil, err
	}

	ev.NotaryRequest = ne.Raw()

	return ev, nil
}
