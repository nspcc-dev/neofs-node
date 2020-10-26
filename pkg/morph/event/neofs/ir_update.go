package neofs

import (
	"crypto/elliptic"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
)

type UpdateInnerRing struct {
	keys []*keys.PublicKey
}

// MorphEvent implements Neo:Morph Event interface.
func (UpdateInnerRing) MorphEvent() {}

func (u UpdateInnerRing) Keys() []*keys.PublicKey { return u.keys }

func ParseUpdateInnerRing(params []stackitem.Item) (event.Event, error) {
	var (
		ev  UpdateInnerRing
		err error
	)

	if ln := len(params); ln != 1 {
		return nil, event.WrongNumberOfParameters(1, ln)
	}

	// parse keys
	irKeys, err := client.ArrayFromStackItem(params[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get updated inner ring keys")
	}

	ev.keys = make([]*keys.PublicKey, 0, len(irKeys))
	for i := range irKeys {
		rawKey, err := client.BytesFromStackItem(irKeys[i])
		if err != nil {
			return nil, errors.Wrap(err, "could not get updated inner ring public key")
		}

		key, err := keys.NewPublicKeyFromBytes(rawKey, elliptic.P256())
		if err != nil {
			return nil, errors.Wrap(err, "could not parse updated inner ring public key")
		}

		ev.keys = append(ev.keys, key)
	}

	return ev, nil
}
