package netmap

import (
	"crypto/elliptic"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
)

type UpdatePeer struct {
	publicKey *keys.PublicKey
	status    uint32
}

// MorphEvent implements Neo:Morph Event interface.
func (UpdatePeer) MorphEvent() {}

func (s UpdatePeer) Status() uint32 {
	return s.status
}

func (s UpdatePeer) PublicKey() *keys.PublicKey {
	return s.publicKey
}

func ParseUpdatePeer(prms []stackitem.Item) (event.Event, error) {
	var (
		ev  UpdatePeer
		err error
	)

	if ln := len(prms); ln != 2 {
		return nil, event.WrongNumberOfParameters(2, ln)
	}

	// parse public key
	key, err := client.BytesFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get public key")
	}

	ev.publicKey, err = keys.NewPublicKeyFromBytes(key, elliptic.P256())
	if err != nil {
		return nil, errors.Wrap(err, "could not parse public key")
	}

	// parse node status
	st, err := client.IntFromStackItem(prms[1])
	if err != nil {
		return nil, errors.Wrap(err, "could not get node status")
	}

	ev.status = uint32(st)

	return ev, nil
}
