package engine

import (
	"errors"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"go.uber.org/zap"
)

func (e *StorageEngine) exists(addr *objectSDK.Address) (bool, error) {
	shPrm := new(shard.ExistsPrm).WithAddress(addr)
	alreadyRemoved := false
	exists := false

	e.iterateOverSortedShards(addr, func(_ int, sh *shard.Shard) (stop bool) {
		res, err := sh.Exists(shPrm)
		if err != nil {
			if errors.Is(err, object.ErrAlreadyRemoved) {
				alreadyRemoved = true

				return true
			}

			// TODO: smth wrong with shard, need to be processed
			e.log.Warn("could not check existence of object in shard",
				zap.Stringer("shard", sh.ID()),
				zap.String("error", err.Error()),
			)
		}

		if res != nil && !exists {
			exists = res.Exists()
		}

		return false
	})

	if alreadyRemoved {
		return false, object.ErrAlreadyRemoved
	}

	return exists, nil
}
