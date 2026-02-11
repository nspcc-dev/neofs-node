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

	args, err := event.GetArgs(ne, putArgCnt)
	if err != nil {
		return nil, err
	}

	epoch, err := event.GetValueFromArg(args, 0, ne.Type().String(), scparser.GetInt64FromInstr)
	if err != nil {
		return nil, err
	}
	ev.setEpoch(uint64(epoch))

	peerID, err := event.GetValueFromArg(args, 1, ne.Type().String(), scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}
	err = ev.setPeerID(peerID)
	if err != nil {
		return nil, event.WrapInvalidArgError(1, ne.Type().String(), err)
	}

	value, err := event.GetValueFromArg(args, 2, ne.Type().String(), scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}
	err = ev.setValue(value)
	if err != nil {
		return nil, event.WrapInvalidArgError(2, ne.Type().String(), err)
	}

	ev.notaryRequest = ne.Raw()

	return *ev, nil
}
