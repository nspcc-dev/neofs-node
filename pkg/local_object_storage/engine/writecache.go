package engine

import (
	"fmt"

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

// FlushWriteCaches flushes write-cache on all shards.
func (e *StorageEngine) FlushWriteCaches() error {
	for _, sh := range e.unsortedShards() {
		err := sh.FlushWriteCache(false)
		if err != nil {
			return fmt.Errorf("flush write-cache for shard %s: %w", sh.ID(), err)
		}
	}
	return nil
}
