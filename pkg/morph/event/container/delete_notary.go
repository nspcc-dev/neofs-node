package container

import (
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
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
	// order on stack is reversed
	(*Delete).setToken,
	(*Delete).setSignature,
	(*Delete).setContainerID,
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
	var (
		ev        Delete
		currentOp opcode.Opcode
	)

	fieldNum := 0

	for _, op := range ne.Params() {
		currentOp = op.Code()

		switch {
		case opcode.PUSHDATA1 <= currentOp && currentOp <= opcode.PUSHDATA4:
			if fieldNum == expectedItemNumDelete {
				return nil, event.UnexpectedArgNumErr(DeleteNotaryEvent)
			}

			deleteFieldSetters[fieldNum](&ev, op.Param())
			fieldNum++
		default:
			return nil, event.UnexpectedOpcode(DeleteNotaryEvent, op.Code())
		}
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
