package neofs

import (
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

type Unbind struct {
	bindCommon
}

func ParseUnbind(params []stackitem.Item) (event.Event, error) {
	var (
		ev  Unbind
		err error
	)

	err = parseBind(&ev.bindCommon, params)
	if err != nil {
		return nil, err
	}

	return ev, nil
}
