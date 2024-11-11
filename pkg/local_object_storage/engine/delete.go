package engine

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Delete marks the objects to be removed.
//
// Returns an error if executions are blocked (see BlockExecution).
//
// NOTE: This is a forced removal, marks any object to be deleted (despite
// any prohibitions on operations with that object).
func (e *StorageEngine) Delete(addr oid.Address) error {
	return e.execIfNotBlocked(func() error {
		return e.deleteObj(addr, true)
	})
}

func (e *StorageEngine) deleteObj(addr oid.Address, force bool) error {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddDeleteDuration)()
	}

	if !force {
		locked, err := e.isLocked(addr)
		if err != nil {
			e.log.Warn("deleting an object without full locking check",
				zap.Error(err),
				zap.Stringer("addr", addr))
		} else if locked {
			return apistatus.ObjectLocked{}
		}
	}

	var inhumePrm shard.InhumePrm
	inhumePrm.MarkAsGarbage(addr)
	if force {
		inhumePrm.ForceRemoval()
	}

	_, err := e.inhumeAddr(addr, inhumePrm)

	return err
}
