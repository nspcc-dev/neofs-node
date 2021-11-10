package policer

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

type jobQueue struct {
	localStorage *engine.StorageEngine
}

func (q *jobQueue) Select(cursor *engine.Cursor, count uint32) ([]*object.Address, *engine.Cursor, error) {
	prm := new(engine.ListWithCursorPrm)
	prm.WithCursor(cursor)
	prm.WithCount(count)

	res, err := q.localStorage.ListWithCursor(prm)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot list objects in engine: %w", err)
	}

	return res.AddressList(), res.Cursor(), nil
}
