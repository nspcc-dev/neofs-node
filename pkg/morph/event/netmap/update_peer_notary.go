package netmap

import (
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

const (
	// UpdateStateNotaryEvent is method name for netmap state updating
	// operations in `Netmap` contract. Is used as identificator for
	// notary delete container requests.
	UpdateStateNotaryEvent = "updateState"
)

// ParseUpdatePeerNotary from NotaryEvent into netmap event structure.
func ParseUpdatePeerNotary(ne event.NotaryEvent) (event.Event, error) {
	const expectedItemNumUpdatePeer = 2
	var (
		ev  UpdatePeer
		err error
	)
	args, err := event.GetArgs(ne, expectedItemNumUpdatePeer)
	if err != nil {
		return nil, err
	}

	state, err := event.GetValueFromArg(args, 0, ne.Type().String(), scparser.GetInt64FromInstr)
	if err != nil {
		return nil, err
	}
	err = ev.decodeState(state)
	if err != nil {
		return nil, event.WrapInvalidArgError(0, ne.Type().String(), err)
	}

	ev.publicKey, err = event.GetValueFromArg(args, 1, ne.Type().String(), scparser.GetPublicKeyFromInstr)
	if err != nil {
		return nil, err
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
