package policer

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
)

type jobQueue struct {
	localStorage *engine.StorageEngine
}

func (q *jobQueue) Select(limit int) ([]*object.Address, error) {
	// TODO: optimize the logic for selecting objects
	// We can prioritize objects for migration, newly arrived objects, etc.
	// It is recommended to make changes after updating the metabase

	res, err := engine.List(q.localStorage, 0) // consider some limit
	if err != nil {
		return nil, err
	}

	rand.New().Shuffle(len(res), func(i, j int) {
		res[i], res[j] = res[j], res[i]
	})

	if len(res) < limit {
		return res, nil
	}

	return res[:limit], nil
}
