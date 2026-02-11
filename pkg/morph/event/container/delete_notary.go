package container

import (
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

func (d *Delete) setContainerID(v []byte) {
	d.containerID = v
}

func (d *Delete) setSignature(v []byte) {
	d.signature = v
}

func (d *Delete) setToken(v []byte) {
	d.token = v
}

var deleteFieldSetters = []func(*Delete, []byte){
	(*Delete).setContainerID,
	(*Delete).setSignature,
	(*Delete).setToken,
}

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

	for i := range args {
		v, err := event.GetValueFromArg(args, i, ne.Type().String(), scparser.GetBytesFromInstr)
		if err != nil {
			return nil, err
		}
		deleteFieldSetters[i](&ev, v)
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
