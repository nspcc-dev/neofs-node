package engine

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
)

// Info groups the information about StorageEngine.
type Info struct {
	Shards []shard.Info
}

// DumpInfo returns information about the StorageEngine.
func (e *StorageEngine) DumpInfo() (i Info) {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	i.Shards = make([]shard.Info, 0, len(e.shards))

	for _, sh := range e.shards {
		i.Shards = append(i.Shards, sh.DumpInfo())
	}

	return
}
