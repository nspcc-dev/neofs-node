package reputation

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

func (p *Put) setEpoch(v uint64) {
	p.epoch = v
}

func (p *Put) setPeerID(v []byte) error {
	if ln := len(v); ln != peerIDLength {
		return fmt.Errorf("peer ID is %d byte long, expected %d", ln, peerIDLength)
	}

	p.peerID.SetPublicKey(v)

	return nil
}

func (p *Put) setValue(v []byte) error {
	return p.value.Unmarshal(v)
}

const (
	// PutNotaryEvent is method name for reputation put operations
	// in `Reputation` contract. Is used as identifier for notary
	// put reputation requests.
	PutNotaryEvent = "put"
)

// ParsePutNotary from NotaryEvent into reputation event structure.
func ParsePutNotary(ne event.NotaryEvent) (event.Event, error) {
	const putArgCnt = 3
	var ev = new(Put)

	args := ne.Params()
	if len(args) != putArgCnt {
		return nil, event.WrongNumberOfParameters(putArgCnt, len(args))
	}

	epoch, err := scparser.GetInt64FromInstr(args[0].Instruction)
	if err != nil {
		return nil, fmt.Errorf("epoch: %w", err)
	}
	ev.setEpoch(uint64(epoch))

	peerID, err := scparser.GetBytesFromInstr(args[1].Instruction)
	if err != nil {
		return nil, fmt.Errorf("peer ID: %w", err)
	}
	err = ev.setPeerID(peerID)
	if err != nil {
		return nil, fmt.Errorf("peer ID: %w", err)
	}

	value, err := scparser.GetBytesFromInstr(args[2].Instruction)
	if err != nil {
		return nil, fmt.Errorf("value: %w", err)
	}
	err = ev.setValue(value)
	if err != nil {
		return nil, fmt.Errorf("value: %w", err)
	}

	ev.notaryRequest = ne.Raw()

	return *ev, nil
}
