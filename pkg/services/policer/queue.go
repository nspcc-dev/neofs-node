package policer

import (
	"fmt"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
)

type jobQueue struct {
	localStorage *engine.StorageEngine
}

func (q *jobQueue) Select(cursor *engine.Cursor, count uint32) ([]objectcore.AddressWithType, *engine.Cursor, error) {
	var prm engine.ListWithCursorPrm
	prm.WithCursor(cursor)
	prm.WithCount(count)

	res, err := q.localStorage.ListWithCursor(prm)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot list objects in engine: %w", err)
	}

	return res.AddressList(), res.Cursor(), nil
}
