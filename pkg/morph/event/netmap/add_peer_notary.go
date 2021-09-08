package netmap

import (
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

func (s *AddPeer) setNode(v []byte) {
	if v != nil {
		s.node = v
	}
}

const (
	// AddPeerNotaryEvent is method name for netmap `addPeer` operation
	// in `Netmap` contract. Is used as identificator for notary
	// peer addition requests.
	AddPeerNotaryEvent = "addPeer"
)

// ParseAddPeerNotary from NotaryEvent into netmap event structure.
func ParseAddPeerNotary(ne event.NotaryEvent) (event.Event, error) {
	var (
		ev        AddPeer
		currentOp opcode.Opcode
	)

	fieldNum := 0

	for _, op := range ne.Params() {
		currentOp = op.Code()

		switch {
		case opcode.PUSHDATA1 <= currentOp && currentOp <= opcode.PUSHDATA4:
			if fieldNum == expectedItemNumAddPeer {
				return nil, event.UnexpectedArgNumErr(AddPeerNotaryEvent)
			}

			ev.setNode(op.Param())
			fieldNum++
		default:
			return nil, event.UnexpectedOpcode(AddPeerNotaryEvent, currentOp)
		}
	}

	return ev, nil
}
