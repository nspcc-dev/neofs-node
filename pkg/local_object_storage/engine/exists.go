package engine

import (
	"errors"

	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

func (e *StorageEngine) exists(addr oid.Address) (bool, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddExistsDuration)()
	}

	for _, sh := range e.sortedShards(addr) {
		exists, err := sh.Exists(addr, false)
		if err != nil {
			if shard.IsErrRemoved(err) {
				return false, apistatus.ObjectAlreadyRemoved{}
			}

			if errors.Is(err, ierrors.ErrParentObject) {
				return false, nil
			}

			if shard.IsErrObjectExpired(err) {
				return false, nil
			}

			if !shard.IsErrNotFound(err) {
				e.reportShardError(sh, "could not check existence of object in shard", err)
			}
			continue
		}

		if exists {
			return true, nil
		}
	}

	return false, nil
}
