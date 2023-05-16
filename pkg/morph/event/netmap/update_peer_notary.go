package netmap

import (
	"crypto/elliptic"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

var errNilPubKey = errors.New("could not parse public key: public key is nil")

func (s *UpdatePeer) setPublicKey(v []byte) (err error) {
	if v == nil {
		return errNilPubKey
	}

	s.publicKey, err = keys.NewPublicKeyFromBytes(v, elliptic.P256())
	if err != nil {
		return fmt.Errorf("could not parse public key: %w", err)
	}

	return
}

const (
	// UpdateStateNotaryEvent is method name for netmap state updating
	// operations in `Netmap` contract. Is used as identificator for
	// notary delete container requests.
	UpdateStateNotaryEvent = "updateState"
)

// ParseUpdatePeerNotary from NotaryEvent into netmap event structure.
func ParseUpdatePeerNotary(ne event.NotaryEvent) (event.Event, error) {
	const expectedItemNumUpdatePeer = 2
	var (
		ev  UpdatePeer
		err error

		currCode opcode.Opcode
	)

	fieldNum := 0

	for _, op := range ne.Params() {
		currCode = op.Code()

		switch {
		case fieldNum == 0 && opcode.PUSHDATA1 <= currCode && currCode <= opcode.PUSHDATA4:
			err = ev.setPublicKey(op.Param())
			if err != nil {
				return nil, err
			}

			fieldNum++
		case fieldNum == 1:
			state, err := event.IntFromOpcode(op)
			if err != nil {
				return nil, err
			}

			err = ev.decodeState(state)
			if err != nil {
				return nil, err
			}

			fieldNum++
		case fieldNum == expectedItemNumUpdatePeer:
			return nil, event.UnexpectedArgNumErr(UpdateStateNotaryEvent)
		default:
			return nil, event.UnexpectedOpcode(UpdateStateNotaryEvent, currCode)
		}
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
