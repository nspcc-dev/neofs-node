package engine

import (
	"slices"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
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
	return e.processAddrDelete(addr, func(sh *shard.Shard, cnr cid.ID, addrs []oid.ID) error {
		return sh.MarkGarbage(cnr, addrs)
	})
}

// DeleteRedundantCopies marks redundant object copies to be removed from all
// listed shards except the most preferred one according to HRW ordering.
//
// Returns an error if executions are blocked (see BlockExecution) or if none of
// the provided shards is found in the engine.
func (e *StorageEngine) DeleteRedundantCopies(addr oid.Address, shardIDs []string) error {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddDeleteDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return e.blockErr
	}

	if len(shardIDs) < 2 {
		return nil
	}

	var deleteShards []shardWrapper
	keep := true
	for _, sh := range e.sortedShards(addr.Object()) {
		if !slices.Contains(shardIDs, sh.ID().String()) {
			continue
		}

		if keep {
			keep = false
			continue
		}

		deleteShards = append(deleteShards, sh)
	}

	if keep {
		return errShardNotFound
	}

	if len(deleteShards) == 0 {
		return nil
	}

	return e.processAddrDeleteOnShards(deleteShards, addr, func(sh *shard.Shard, cnr cid.ID, addrs []oid.ID) error {
		return sh.MarkGarbage(cnr, addrs)
	})
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
