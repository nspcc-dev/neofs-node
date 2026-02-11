package container

import (
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

const (
	// DeleteNotaryEvent is method name for container delete operations
	// in `Container` contract. Is used as identificator for notary
	// delete container requests.
	DeleteNotaryEvent = "delete"
)

// ParseDeleteNotary from NotaryEvent into container event structure.
func ParseDeleteNotary(ne event.NotaryEvent) (event.Event, error) {
	const expectedItemNumDelete = 3
	var ev Delete

	args, err := event.GetArgs(ne, expectedItemNumDelete)
	if err != nil {
		return nil, err
	}

	ev.containerID, err = event.GetValueFromArg(args, 0, ne.Type().String(), scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}
	ev.signature, err = event.GetValueFromArg(args, 1, ne.Type().String(), scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}
	ev.token, err = event.GetValueFromArg(args, 2, ne.Type().String(), scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
