package neofs

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result/subscriptions"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

type Bind struct {
	bindCommon
}

type bindCommon struct {
	user []byte
	keys [][]byte

	// txHash is used in notary environmental
	// for calculating unique but same for
	// all notification receivers values.
	txHash util.Uint256
}

// TxHash returns hash of the TX with new epoch
// notification.
func (b bindCommon) TxHash() util.Uint256 {
	return b.txHash
}

// MorphEvent implements Neo:Morph Event interface.
func (bindCommon) MorphEvent() {}

func (b bindCommon) Keys() [][]byte { return b.keys }

func (b bindCommon) User() []byte { return b.user }

func ParseBind(e *subscriptions.NotificationEvent) (event.Event, error) {
	var (
		ev  Bind
		err error
	)

	params, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("could not parse stack items from notify event: %w", err)
	}

	err = parseBind(&ev.bindCommon, params)
	if err != nil {
		return nil, err
	}

	ev.txHash = e.Container

	return ev, nil
}

func parseBind(dst *bindCommon, params []stackitem.Item) error {
	if ln := len(params); ln != 2 {
		return event.WrongNumberOfParameters(2, ln)
	}

	var err error

	// parse user
	dst.user, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return fmt.Errorf("could not get bind user: %w", err)
	}

	// parse keys
	bindKeys, err := client.ArrayFromStackItem(params[1])
	if err != nil {
		return fmt.Errorf("could not get bind keys: %w", err)
	}

	dst.keys = make([][]byte, 0, len(bindKeys))

	for i := range bindKeys {
		rawKey, err := client.BytesFromStackItem(bindKeys[i])
		if err != nil {
			return fmt.Errorf("could not get bind public key: %w", err)
		}

		dst.keys = append(dst.keys, rawKey)
	}

	return nil
}
