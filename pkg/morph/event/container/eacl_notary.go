package container

import (
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

func (x *SetEACL) setTable(v []byte) {
	if v != nil {
		x.table = v
	}
}

func (x *SetEACL) setSignature(v []byte) {
	if v != nil {
		x.signature = v
	}
}

func (x *SetEACL) setPublicKey(v []byte) {
	if v != nil {
		x.publicKey = v
	}
}

func (x *SetEACL) setToken(v []byte) {
	if v != nil {
		x.token = v
	}
}

var setEACLFieldSetters = []func(*SetEACL, []byte){
	// order on stack is reversed
	(*SetEACL).setToken,
	(*SetEACL).setPublicKey,
	(*SetEACL).setSignature,
	(*SetEACL).setTable,
}

const (
	// SetEACLNotaryEvent is method name for container EACL operations
	// in `Container` contract. Is used as identificator for notary
	// EACL changing requests.
	SetEACLNotaryEvent = "setEACL"
)

// ParseSetEACLNotary from NotaryEvent into container event structure.
func ParseSetEACLNotary(ne event.NotaryEvent) (event.Event, error) {
	var (
		ev        SetEACL
		currentOp opcode.Opcode
	)

	fieldNum := 0

	for _, op := range ne.Params() {
		currentOp = op.Code()

		switch {
		case opcode.PUSHDATA1 <= currentOp && currentOp <= opcode.PUSHDATA4:
			if fieldNum == expectedItemNumEACL {
				return nil, event.UnexpectedArgNumErr(SetEACLNotaryEvent)
			}

			setEACLFieldSetters[fieldNum](&ev, op.Param())
			fieldNum++
		default:
			return nil, event.UnexpectedOpcode(SetEACLNotaryEvent, op.Code())
		}
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
