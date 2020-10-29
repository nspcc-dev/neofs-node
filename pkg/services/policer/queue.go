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

	// FIXME: add the ability to limit Select result
	res, err := q.localStorage.Select(object.SearchFilters{})
	if err != nil {
		return nil, err
	}

	if len(res) < limit {
		return res, nil
	}

	return res[:limit], nil
}
