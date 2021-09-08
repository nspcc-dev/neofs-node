package reputation

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

func (p *Put) setEpoch(v uint64) {
	p.epoch = v
}

func (p *Put) setPeerID(v []byte) error {
	if ln := len(v); ln != peerIDLength {
		return fmt.Errorf("peer ID is %d byte long, expected %d", ln, peerIDLength)
	}

	var publicKey [33]byte
	copy(publicKey[:], v)
	p.peerID.SetPublicKey(publicKey)

	return nil
}

func (p *Put) setValue(v []byte) error {
	return p.value.Unmarshal(v)
}

var fieldSetters = []func(*Put, []byte) error{
	// order on stack is reversed
	(*Put).setValue,
	(*Put).setPeerID,
}

const (
	// PutNotaryEvent is method name for reputation put operations
	// in `Reputation` contract. Is used as identifier for notary
	// put reputation requests.
	PutNotaryEvent = "put"
)

// ParsePutNotary from NotaryEvent into reputation event structure.
func ParsePutNotary(ne event.NotaryEvent) (event.Event, error) {
	var ev Put

	fieldNum := 0

	for _, op := range ne.Params() {
		switch fieldNum {
		case 0, 1:
			data, err := event.BytesFromOpcode(op)
			if err != nil {
				return nil, err
			}

			err = fieldSetters[fieldNum](&ev, data)
			if err != nil {
				return nil, fmt.Errorf("can't parse field num %d: %w", fieldNum, err)
			}
		case 2:
			n, err := event.IntFromOpcode(op)
			if err != nil {
				return nil, err
			}

			ev.setEpoch(uint64(n))
		default:
			return nil, event.UnexpectedArgNumErr(PutNotaryEvent)
		}
		fieldNum++
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
