package neofs

import (
	"crypto/elliptic"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
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
		return nil, fmt.Errorf("could not get bind user: %w", err)
	}

	ev.user, err = util.Uint160DecodeBytesBE(user)
	if err != nil {
		return nil, fmt.Errorf("could not convert bind user to uint160: %w", err)
	}

	// parse keys
	bindKeys, err := client.ArrayFromStackItem(params[1])
	if err != nil {
		return nil, fmt.Errorf("could not get bind keys: %w", err)
	}

	ev.keys = make([]*keys.PublicKey, 0, len(bindKeys))
	for i := range bindKeys {
		rawKey, err := client.BytesFromStackItem(bindKeys[i])
		if err != nil {
			return nil, fmt.Errorf("could not get bind public key: %w", err)
		}

		key, err := keys.NewPublicKeyFromBytes(rawKey, elliptic.P256())
		if err != nil {
			return nil, fmt.Errorf("could not parse bind public key: %w", err)
		}

		ev.keys = append(ev.keys, key)
	}

	return ev, nil
}
