package engine

import "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"

// RestoreShard restores objects from dump to the shard with provided identifier.
//
// Returns an error if shard is not read-only.
func (e *StorageEngine) RestoreShard(id *shard.ID, prm shard.RestorePrm) error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	sh, ok := e.shards[id.String()]
	if !ok {
		return errShardNotFound
	}

	_, err := sh.Restore(prm)
	return err
}
