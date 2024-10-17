package engine

import (
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// ReviveShardStatus contains the Status of the object's revival in the Shard, Shard ID and Error.
type ReviveShardStatus struct {
	ID     string
	Status meta.ReviveStatus
	Error  error
}

// ReviveStatus represents the status of the object's revival in the StorageEngine.
type ReviveStatus struct {
	Shards []ReviveShardStatus
}

// ReviveObject forcefully revives object by oid.Address in the StorageEngine.
// Iterate over all shards despite errors and purge all removal marks from all metabases.
func (e *StorageEngine) ReviveObject(address oid.Address) (res ReviveStatus, err error) {
	e.iterateOverUnsortedShards(func(sh hashedShard) (stop bool) {
		reviveStatus, err := sh.ReviveObject(address)
		id := *sh.ID()
		res.Shards = append(res.Shards, ReviveShardStatus{
			ID:     id.String(),
			Status: reviveStatus,
			Error:  err,
		})
		if err != nil {
			e.log.Debug("failed to revive object in shard", zap.String("shard", id.String()), zap.Error(err))
		}

		return false
	})
	return
}
