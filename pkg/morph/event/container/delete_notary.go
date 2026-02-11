package container

import (
	"fmt"

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

	args := ne.Params()
	if len(args) != expectedItemNumDelete {
		return nil, event.WrongNumberOfParameters(expectedItemNumDelete, len(args))
	}

	for i, arg := range args {
		v, err := scparser.GetBytesFromInstr(arg.Instruction)
		if err != nil {
			return nil, fmt.Errorf("%s arg #%d: %w", DeleteNotaryEvent, i, err)
		}
		deleteFieldSetters[i](&ev, v)
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
