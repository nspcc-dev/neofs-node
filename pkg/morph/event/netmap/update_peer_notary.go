package netmap

import (
	"fmt"

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
	args := ne.Params()
	if len(args) != expectedItemNumUpdatePeer {
		return nil, event.WrongNumberOfParameters(expectedItemNumUpdatePeer, len(args))
	}

	state, err := scparser.GetInt64FromInstr(args[0].Instruction)
	if err != nil {
		return nil, fmt.Errorf("state: %w", err)
	}
	err = ev.decodeState(state)
	if err != nil {
		return nil, fmt.Errorf("state: %w", err)
	}

	ev.publicKey, err = scparser.GetPublicKeyFromInstr(args[1].Instruction)
	if err != nil {
		return nil, fmt.Errorf("public key: %w", err)
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
