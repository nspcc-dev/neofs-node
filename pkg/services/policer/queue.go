package policer

import (
	"fmt"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
)

type jobQueue struct {
	localStorage localStorage
}

func (q *jobQueue) Select(cursor *engine.Cursor, count uint32) ([]objectcore.AddressWithType, *engine.Cursor, error) {
	res, cursor, err := q.localStorage.ListWithCursor(count, cursor)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot list objects in engine: %w", err)
	}

	return res, cursor, nil
}
