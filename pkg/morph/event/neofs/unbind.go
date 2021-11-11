package neofs

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result/subscriptions"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

type Unbind struct {
	bindCommon
}

func ParseUnbind(e *subscriptions.NotificationEvent) (event.Event, error) {
	var (
		ev  Unbind
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
