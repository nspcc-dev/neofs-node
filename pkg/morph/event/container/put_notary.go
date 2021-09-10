package container

import (
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

func (p *Put) setRawContainer(v []byte) {
	if v != nil {
		p.rawContainer = v
	}
}

func (p *Put) setSignature(v []byte) {
	if v != nil {
		p.signature = v
	}
}

func (p *Put) setPublicKey(v []byte) {
	if v != nil {
		p.publicKey = v
	}
}

func (p *Put) setToken(v []byte) {
	if v != nil {
		p.token = v
	}
}

var putFieldSetters = []func(*Put, []byte){
	// order on stack is reversed
	(*Put).setToken,
	(*Put).setPublicKey,
	(*Put).setSignature,
	(*Put).setRawContainer,
}

const (
	// PutNotaryEvent is method name for container put operations
	// in `Container` contract. Is used as identificator for notary
	// put container requests.
	PutNotaryEvent = "put"
)

// ParsePutNotary from NotaryEvent into container event structure.
func ParsePutNotary(ne event.NotaryEvent) (event.Event, error) {
	var (
		ev        Put
		currentOp opcode.Opcode
	)

	fieldNum := 0

	for _, op := range ne.Params() {
		currentOp = op.Code()

		switch {
		case opcode.PUSHDATA1 <= currentOp && currentOp <= opcode.PUSHDATA4:
			if fieldNum == expectedItemNumPut {
				return nil, event.UnexpectedArgNumErr(PutNotaryEvent)
			}

			putFieldSetters[fieldNum](&ev, op.Param())
			fieldNum++
		default:
			return nil, event.UnexpectedOpcode(PutNotaryEvent, op.Code())
		}
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
