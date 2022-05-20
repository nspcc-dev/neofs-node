package engine

import "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"

// DumpShard dumps objects from the shard with provided identifier.
//
// Returns an error if shard is not read-only.
func (e *StorageEngine) DumpShard(id *shard.ID, prm shard.DumpPrm) error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	sh, ok := e.shards[id.String()]
	if !ok {
		return errShardNotFound
	}

	_, err := sh.Dump(prm)
	return err
}
