package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

func (x *SetEACL) setTable(v []byte) {
	x.table = v
}

func (x *SetEACL) setSignature(v []byte) {
	x.signature = v
}

func (x *SetEACL) setPublicKey(v []byte) {
	x.publicKey = v
}

func (x *SetEACL) setToken(v []byte) {
	x.token = v
}

var setEACLFieldSetters = []func(*SetEACL, []byte){
	(*SetEACL).setTable,
	(*SetEACL).setSignature,
	(*SetEACL).setPublicKey,
	(*SetEACL).setToken,
}

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

	args := ne.Params()
	if len(args) != expectedItemNumEACL {
		return nil, event.WrongNumberOfParameters(expectedItemNumEACL, len(args))
	}
	for i, arg := range args {
		v, err := scparser.GetBytesFromInstr(arg.Instruction)
		if err != nil {
			return nil, fmt.Errorf("%s arg #%d: %w", SetEACLNotaryEvent, i, err)
		}
		setEACLFieldSetters[i](&ev, v)
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
