package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
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

	// PutNotaryEvent is an ID of notary "put named container" notification.
	PutNamedNotaryEvent = "putNamed"
)

func parsePutNotary(ev *Put, raw *payload.P2PNotaryRequest, ops []event.Op) error {
	var (
		currentOp opcode.Opcode
		fieldNum  = 0
	)

	for _, op := range ops {
		currentOp = op.Code()

		switch {
		case opcode.PUSHDATA1 <= currentOp && currentOp <= opcode.PUSHDATA4:
			if fieldNum == expectedItemNumPut {
				return event.UnexpectedArgNumErr(PutNotaryEvent)
			}

			putFieldSetters[fieldNum](ev, op.Param())
			fieldNum++
		default:
			return event.UnexpectedOpcode(PutNotaryEvent, op.Code())
		}
	}

	ev.notaryRequest = raw

	return nil
}

// ParsePutNotary from NotaryEvent into container event structure.
func ParsePutNotary(ne event.NotaryEvent) (event.Event, error) {
	var ev Put

	err := parsePutNotary(&ev, ne.Raw(), ne.Params())
	if err != nil {
		return nil, err
	}

	return ev, nil
}

// ParsePutNamedNotary parses PutNamed event structure from generic event.NotaryEvent.
func ParsePutNamedNotary(ne event.NotaryEvent) (event.Event, error) {
	ops := ne.Params()

	const putNamedAdditionalArgs = 2 // PutNamed has same args as Put + (name, zone) (2)

	if len(ops) != expectedItemNumPut+putNamedAdditionalArgs {
		return nil, event.UnexpectedArgNumErr(PutNamedNotaryEvent)
	}

	var (
		ev  PutNamed
		err error
	)

	ev.zone, err = event.StringFromOpcode(ops[0])
	if err != nil {
		return nil, fmt.Errorf("parse arg zone: %w", err)
	}

	ev.name, err = event.StringFromOpcode(ops[1])
	if err != nil {
		return nil, fmt.Errorf("parse arg name: %w", err)
	}

	err = parsePutNotary(&ev.Put, ne.Raw(), ops[putNamedAdditionalArgs:])
	if err != nil {
		return nil, err
	}

	return ev, nil
}
