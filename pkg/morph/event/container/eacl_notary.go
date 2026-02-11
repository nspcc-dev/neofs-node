package container

import (
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

	args, err := event.GetArgs(ne, expectedItemNumEACL)
	if err != nil {
		return nil, err
	}
	for i := range args {
		v, err := event.GetValueFromArg(args, i, ne.Type().String(), scparser.GetBytesFromInstr)
		if err != nil {
			return nil, err
		}
		setEACLFieldSetters[i](&ev, v)
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
