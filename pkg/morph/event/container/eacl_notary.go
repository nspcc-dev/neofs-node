package container

import (
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

const (
	// SetEACLNotaryEvent is method name for container EACL operations
	// in `Container` contract. Is used as identificator for notary
	// EACL changing requests.
	SetEACLNotaryEvent = "setEACL"
)

// ParseSetEACLNotary from NotaryEvent into container event structure.
func ParseSetEACLNotary(ne event.NotaryEvent) (event.Event, error) {
	const expectedItemNumEACL = 4
	var ev SetEACL

	args, err := event.GetArgs(ne, expectedItemNumEACL)
	if err != nil {
		return nil, err
	}

	ev.table, err = event.GetValueFromArg(args, 0, ne.Type().String(), scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}
	ev.signature, err = event.GetValueFromArg(args, 1, ne.Type().String(), scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}
	ev.publicKey, err = event.GetValueFromArg(args, 2, ne.Type().String(), scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}
	ev.token, err = event.GetValueFromArg(args, 3, ne.Type().String(), scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
