package engine

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/nspcc-dev/hrw"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/pkg/errors"
)

// AddShard adds a new shard to the storage engine.
//
// Returns any error encountered that did not allow adding a shard.
// Otherwise returns the ID of the added shard.
func (e *StorageEngine) AddShard(opts ...shard.Option) (*shard.ID, error) {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	id, err := e.generateShardID()
	if err != nil {
		return nil, errors.Wrap(err, "could not generate shard ID")
	}

	e.shards[id.String()] = shard.New(append(opts, shard.WithID(id))...)

	return id, nil
}

func (e *StorageEngine) generateShardID() (*shard.ID, error) {
	uid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	bin, err := uid.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return shard.NewIDFromBytes(bin), nil
}

func (e *StorageEngine) shardWeight(sh *shard.Shard) float64 {
	weightValues := sh.WeightValues()

	return float64(weightValues.FreeSpace)
}

func (e *StorageEngine) sortShardsByWeight(objAddr fmt.Stringer) []*shard.Shard {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	shards := make([]*shard.Shard, 0, len(e.shards))
	weights := make([]float64, 0, len(e.shards))

	for _, sh := range e.shards {
		shards = append(shards, sh)
		weights = append(weights, e.shardWeight(sh))
	}

	hrw.SortSliceByWeightValue(shards, weights, hrw.Hash([]byte(objAddr.String())))

	return shards
}

func (e *StorageEngine) iterateOverSortedShards(addr *object.Address, handler func(*shard.Shard) (stop bool)) {
	for _, sh := range e.sortShardsByWeight(addr) {
		if handler(sh) {
			break
		}
	}
}
