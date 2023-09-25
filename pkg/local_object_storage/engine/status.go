package engine

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ObjectShardStatus contains the status of the object in the Shard and Shard ID.
type ObjectShardStatus struct {
	ID    string
	Shard shard.ObjectStatus
}

// ObjectStatus represents the status of the object in the StorageEngine.
type ObjectStatus struct {
	Shards []ObjectShardStatus
}

// ObjectStatus returns the status of the object in the StorageEngine. It contains status of the object in all shards.
func (e *StorageEngine) ObjectStatus(address oid.Address) (ObjectStatus, error) {
	var res ObjectStatus
	var err error

	e.iterateOverSortedShards(address, func(_ int, sh hashedShard) (stop bool) {
		var shardStatus shard.ObjectStatus
		shardStatus, err = sh.ObjectStatus(address)
		id := *sh.ID()
		if err == nil {
			res.Shards = append(res.Shards, ObjectShardStatus{
				ID:    id.String(),
				Shard: shardStatus,
			})
		}
		return err != nil
	})
	return res, err
}
