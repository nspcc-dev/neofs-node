package engine

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/nspcc-dev/hrw"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
)

var errShardNotFound = errors.New("shard not found")

type hashedShard shardWrapper

// AddShard adds a new shard to the storage engine.
//
// Returns any error encountered that did not allow adding a shard.
// Otherwise returns the ID of the added shard.
func (e *StorageEngine) AddShard(opts ...shard.Option) (*shard.ID, error) {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	pool, err := ants.NewPool(int(e.shardPoolSize), ants.WithNonblocking(true))
	if err != nil {
		return nil, err
	}

	id, err := generateShardID()
	if err != nil {
		return nil, fmt.Errorf("could not generate shard ID: %w", err)
	}

	sh := shard.New(append(opts,
		shard.WithID(id),
		shard.WithExpiredTombstonesCallback(e.processExpiredTombstones),
		shard.WithExpiredLocksCallback(e.processExpiredLocks),
	)...)

	if err := sh.UpdateID(); err != nil {
		return nil, fmt.Errorf("could not open shard: %w", err)
	}

	strID := sh.ID().String()
	if _, ok := e.shards[strID]; ok {
		return nil, fmt.Errorf("shard with id %s was already added", strID)
	}

	e.shards[strID] = shardWrapper{
		metaErrorCount:  atomic.NewUint32(0),
		writeErrorCount: atomic.NewUint32(0),
		Shard:           sh,
	}

	e.shardPools[strID] = pool

	return sh.ID(), nil
}

func generateShardID() (*shard.ID, error) {
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

func (e *StorageEngine) sortShardsByWeight(objAddr interface{ EncodeToString() string }) []hashedShard {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	shards := make([]hashedShard, 0, len(e.shards))
	weights := make([]float64, 0, len(e.shards))

	for _, sh := range e.shards {
		shards = append(shards, hashedShard(sh))
		weights = append(weights, e.shardWeight(sh.Shard))
	}

	hrw.SortSliceByWeightValue(shards, weights, hrw.Hash([]byte(objAddr.EncodeToString())))

	return shards
}

func (e *StorageEngine) unsortedShards() []hashedShard {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	shards := make([]hashedShard, 0, len(e.shards))

	for _, sh := range e.shards {
		shards = append(shards, hashedShard(sh))
	}

	return shards
}

func (e *StorageEngine) iterateOverSortedShards(addr oid.Address, handler func(int, hashedShard) (stop bool)) {
	for i, sh := range e.sortShardsByWeight(addr) {
		if handler(i, sh) {
			break
		}
	}
}

func (e *StorageEngine) iterateOverUnsortedShards(handler func(hashedShard) (stop bool)) {
	for _, sh := range e.unsortedShards() {
		if handler(sh) {
			break
		}
	}
}

// SetShardMode sets mode of the shard with provided identifier.
//
// Returns an error if shard mode was not set, or shard was not found in storage engine.
func (e *StorageEngine) SetShardMode(id *shard.ID, m shard.Mode, resetErrorCounter bool) error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	for shID, sh := range e.shards {
		if id.String() == shID {
			if resetErrorCounter {
				sh.metaErrorCount.Store(0)
				sh.writeErrorCount.Store(0)
			}
			return sh.SetMode(m)
		}
	}

	return errShardNotFound
}

func (s hashedShard) Hash() uint64 {
	return hrw.Hash(
		[]byte(s.Shard.ID().String()),
	)
}
