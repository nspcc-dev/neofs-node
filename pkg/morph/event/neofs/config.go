package neofs

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result/subscriptions"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
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

func ParseConfig(e *subscriptions.NotificationEvent) (event.Event, error) {
	var (
		ev  Config
		err error
	)

	params, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("could not parse stack items from notify event: %w", err)
	}

	if ln := len(params); ln != 3 {
		return nil, event.WrongNumberOfParameters(3, ln)
	}

	// parse id
	ev.id, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, fmt.Errorf("could not get config update id: %w", err)
	}

	// parse key
	ev.key, err = client.BytesFromStackItem(params[1])
	if err != nil {
		return nil, fmt.Errorf("could not get config key: %w", err)
	}

	// parse value
	ev.value, err = client.BytesFromStackItem(params[2])
	if err != nil {
		return nil, fmt.Errorf("could not get config value: %w", err)
	}

	return ev, nil
}
