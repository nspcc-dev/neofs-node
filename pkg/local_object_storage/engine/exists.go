package engine

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

func (e *StorageEngine) exists(addr oid.Address) (bool, error) {
	var shPrm shard.ExistsPrm
	shPrm.WithAddress(addr)
	alreadyRemoved := false
	exists := false

	e.iterateOverSortedShards(addr, func(_ int, sh hashedShard) (stop bool) {
		res, err := sh.Exists(shPrm)
		if err != nil {
			if shard.IsErrRemoved(err) {
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
		var errRemoved apistatus.ObjectAlreadyRemoved

		return false, errRemoved
	}

	return exists, nil
}
