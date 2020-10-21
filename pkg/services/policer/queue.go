package policer

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
)

type jobQueue struct {
	localStorage *localstore.Storage
}

func (q *jobQueue) Select(limit int) ([]*object.Address, error) {
	// TODO: optimize the logic for selecting objects
	// We can prioritize objects for migration, newly arrived objects, etc.
	// It is recommended to make changes after updating the metabase

	res := make([]*object.Address, 0, limit)

	if err := q.localStorage.Iterate(nil, func(meta *localstore.ObjectMeta) bool {
		res = append(res, meta.Head().Address())

		return len(res) >= limit
	}); err != nil {
		return nil, err
	}

	return res, nil
}
