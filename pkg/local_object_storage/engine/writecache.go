package engine

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
)

// FlushWriteCache flushes write-cache on a single shard with the given ID.
func (e *StorageEngine) FlushWriteCache(id *shard.ID) error {
	e.mtx.RLock()
	sh, ok := e.shards[id.String()]
	e.mtx.RUnlock()

	if !ok {
		return errShardNotFound
	}

	return sh.FlushWriteCache(false)
}
