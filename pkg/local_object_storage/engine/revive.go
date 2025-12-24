package engine

import (
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// ReviveShardStatus contains the Status of the object's revival in the Shard and Shard ID.
type ReviveShardStatus struct {
	ID     string
	Status meta.ReviveStatus
}

// ReviveStatus represents the status of the object's revival in the StorageEngine.
type ReviveStatus struct {
	Shards []ReviveShardStatus
}

// ReviveObject forcefully revives object by oid.Address in the StorageEngine.
// Iterate over all shards despite errors and purge all removal marks from all metabases.
func (e *StorageEngine) ReviveObject(address oid.Address) (ReviveStatus, error) {
	var res ReviveStatus

	for _, sh := range e.unsortedShards() {
		reviveStatus, err := sh.ReviveObject(address)
		id := *sh.ID()
		res.Shards = append(res.Shards, ReviveShardStatus{
			ID:     id.String(),
			Status: reviveStatus,
		})
		if err != nil {
			e.log.Warn("failed to revive object in shard",
				zap.Stringer("shard", id),
				zap.Stringer("address", address),
				zap.Error(err),
			)
		}
	}
	return res, nil
}
