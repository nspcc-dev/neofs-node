package engine

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Delete marks the objects to be removed. This is a forced removal, it
// overrides any locks or other reasons to keep the object, but it doesn't
// delete the object immediately (like [StorageEngine.Drop] does), instead it
// just marks the address for later removal by GC according to GC settings.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Delete(addr oid.Address) error {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddDeleteDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return e.blockErr
	}
	return e.inhume([]oid.Address{addr}, true, nil, 0)
}

// Drop removes an object from the storage engine from all shards. This
// function bypasses any lock checks or other reasons to keep the object and
// performs immediate removal, not just marking it to be removed by GC later
// (like [StorageEngine.Delete] does).
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Drop(addr oid.Address) error {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddDropDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return e.blockErr
	}
	return e.processAddrDelete(addr, (*shard.Shard).Delete)
}
