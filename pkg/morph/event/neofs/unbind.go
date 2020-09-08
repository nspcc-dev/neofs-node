package neofs

import (
	"crypto/elliptic"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
)

type Unbind struct {
	user util.Uint160
	keys []*keys.PublicKey
}

// MorphEvent implements Neo:Morph Event interface.
func (Unbind) MorphEvent() {}

func (u Unbind) Keys() []*keys.PublicKey { return u.keys }

func (u Unbind) User() util.Uint160 { return u.user }

func ParseUnbind(params []smartcontract.Parameter) (event.Event, error) {
	var (
		ev  Unbind
		err error
	)

	if ln := len(params); ln != 2 {
		return nil, event.WrongNumberOfParameters(2, ln)
	}

	// parse user
	user, err := client.BytesFromStackParameter(params[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get bind user")
	}

	ev.user, err = util.Uint160DecodeBytesBE(user)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert unbind user to uint160")
	}

	// parse keys
	unbindKeys, err := client.ArrayFromStackParameter(params[1])
	if err != nil {
		return nil, errors.Wrap(err, "could not get unbind keys")
	}

	ev.keys = make([]*keys.PublicKey, 0, len(unbindKeys))
	for i := range unbindKeys {
		rawKey, err := client.BytesFromStackParameter(unbindKeys[i])
		if err != nil {
			return nil, errors.Wrap(err, "could not get unbind public key")
		}

		key, err := keys.NewPublicKeyFromBytes(rawKey, elliptic.P256())
		if err != nil {
			return nil, errors.Wrap(err, "could not parse unbind public key")
		}

		ev.keys = append(ev.keys, key)
	}

	return ev, nil
}
