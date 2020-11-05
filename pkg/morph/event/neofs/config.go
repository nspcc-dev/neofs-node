package neofs

import (
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
)

type Config struct {
	key   []byte
	value []byte
	id    []byte
}

// MorphEvent implements Neo:Morph Event interface.
func (Config) MorphEvent() {}

func (u Config) ID() []byte { return u.id }

func (u Config) Key() []byte { return u.key }

func (u Config) Value() []byte { return u.value }

func ParseConfig(params []stackitem.Item) (event.Event, error) {
	var (
		ev  Config
		err error
	)

	if ln := len(params); ln != 3 {
		return nil, event.WrongNumberOfParameters(3, ln)
	}

	// parse id
	ev.id, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get config update id")
	}

	// parse key
	ev.key, err = client.BytesFromStackItem(params[1])
	if err != nil {
		return nil, errors.Wrap(err, "could not get config key")
	}

	// parse value
	ev.value, err = client.BytesFromStackItem(params[2])
	if err != nil {
		return nil, errors.Wrap(err, "could not get config value")
	}

	return ev, nil
}
