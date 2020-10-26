package neofs

import (
	"crypto/elliptic"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
)

type Bind struct {
	user util.Uint160
	keys []*keys.PublicKey
}

// MorphEvent implements Neo:Morph Event interface.
func (Bind) MorphEvent() {}

func (b Bind) Keys() []*keys.PublicKey { return b.keys }

func (b Bind) User() util.Uint160 { return b.user }

func ParseBind(params []stackitem.Item) (event.Event, error) {
	var (
		ev  Bind
		err error
	)

	if ln := len(params); ln != 2 {
		return nil, event.WrongNumberOfParameters(2, ln)
	}

	// parse user
	user, err := client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get bind user")
	}

	ev.user, err = util.Uint160DecodeBytesBE(user)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert bind user to uint160")
	}

	// parse keys
	bindKeys, err := client.ArrayFromStackItem(params[1])
	if err != nil {
		return nil, errors.Wrap(err, "could not get bind keys")
	}

	ev.keys = make([]*keys.PublicKey, 0, len(bindKeys))
	for i := range bindKeys {
		rawKey, err := client.BytesFromStackItem(bindKeys[i])
		if err != nil {
			return nil, errors.Wrap(err, "could not get bind public key")
		}

		key, err := keys.NewPublicKeyFromBytes(rawKey, elliptic.P256())
		if err != nil {
			return nil, errors.Wrap(err, "could not parse bind public key")
		}

		ev.keys = append(ev.keys, key)
	}

	return ev, nil
}
