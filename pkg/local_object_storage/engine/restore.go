package engine

import (
	"io"

	coreshard "github.com/nspcc-dev/neofs-node/pkg/core/shard"
)

// RestoreShard restores objects from dump to the shard with provided identifier.
//
// Returns an error if shard is not read-only.
func (e *StorageEngine) RestoreShard(id *coreshard.ID, r io.Reader, ignoreErrors bool) error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	sh, ok := e.shards[id.String()]
	if !ok {
		return errShardNotFound
	}

	_, _, err := sh.Restore(r, ignoreErrors)
	return err
}
