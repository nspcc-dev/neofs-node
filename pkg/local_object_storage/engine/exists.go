package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

func (e *StorageEngine) exists(addr *objectSDK.Address) (bool, error) {
	shPrm := new(shard.ExistsPrm).WithAddress(addr)
	alreadyRemoved := false
	exists := false

	e.iterateOverSortedShards(addr, func(_ int, sh hashedShard) (stop bool) {
		res, err := sh.Exists(shPrm)
		if err != nil {
			if errors.Is(err, object.ErrAlreadyRemoved) {
				alreadyRemoved = true

				return true
			}

			e.reportShardError(sh, "could not check existence of object in shard", err)
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
