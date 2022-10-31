package engine

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/nspcc-dev/hrw"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var errShardNotFound = logicerr.Wrap(errors.New("shard not found"))

type hashedShard shardWrapper

type metricsWithID struct {
	id string
	mw MetricRegister
}

func (m *metricsWithID) SetShardID(id string) {
	// concurrent settings are not expected =>
	// no mutex protection
	m.id = id
}

func (m *metricsWithID) SetObjectCounter(objectType string, v uint64) {
	m.mw.SetObjectCounter(m.id, objectType, v)
}

func (m *metricsWithID) AddToObjectCounter(objectType string, delta int) {
	m.mw.AddToObjectCounter(m.id, objectType, delta)
}

func (m *metricsWithID) IncObjectCounter(objectType string) {
	m.mw.AddToObjectCounter(m.id, objectType, +1)
}

func (m *metricsWithID) DecObjectCounter(objectType string) {
	m.mw.AddToObjectCounter(m.id, objectType, -1)
}

// AddShard adds a new shard to the storage engine.
//
// Returns any error encountered that did not allow adding a shard.
// Otherwise returns the ID of the added shard.
func (e *StorageEngine) AddShard(opts ...shard.Option) (*shard.ID, error) {
	sh, err := e.createShard(opts)
	if err != nil {
		return nil, fmt.Errorf("could not create a shard: %w", err)
	}

	err = e.addShard(sh)
	if err != nil {
		return nil, fmt.Errorf("could not add %s shard: %w", sh.ID().String(), err)
	}

	return sh.ID(), nil
}

func (e *StorageEngine) createShard(opts []shard.Option) (*shard.Shard, error) {
	id, err := generateShardID()
	if err != nil {
		return nil, fmt.Errorf("could not generate shard ID: %w", err)
	}

	e.mtx.RLock()

	if e.metrics != nil {
		opts = append(opts, shard.WithMetricsWriter(
			&metricsWithID{
				id: id.String(),
				mw: e.metrics,
			},
		))
	}

	e.mtx.RUnlock()

	sh := shard.New(append(opts,
		shard.WithID(id),
		shard.WithExpiredTombstonesCallback(e.processExpiredTombstones),
		shard.WithExpiredLocksCallback(e.processExpiredLocks),
		shard.WithDeletedLockCallback(e.processDeletedLocks),
	)...)

	if err := sh.UpdateID(); err != nil {
		return nil, fmt.Errorf("could not update shard ID: %w", err)
	}

	return sh, err
}

func (e *StorageEngine) addShard(sh *shard.Shard) error {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	pool, err := ants.NewPool(int(e.shardPoolSize), ants.WithNonblocking(true))
	if err != nil {
		return fmt.Errorf("could not create pool: %w", err)
	}

	strID := sh.ID().String()
	if _, ok := e.shards[strID]; ok {
		return fmt.Errorf("shard with id %s was already added", strID)
	}

	e.shards[strID] = shardWrapper{
		errorCount: atomic.NewUint32(0),
		Shard:      sh,
	}

	e.shardPools[strID] = pool

	return nil
}

// removeShards removes specified shards. Skips non-existent shards.
// Logs errors about shards that it could not Close after the removal.
func (e *StorageEngine) removeShards(ids ...string) {
	if len(ids) == 0 {
		return
	}

	ss := make([]shardWrapper, 0, len(ids))

	e.mtx.Lock()
	for _, id := range ids {
		sh, found := e.shards[id]
		if !found {
			continue
		}

		ss = append(ss, sh)
		delete(e.shards, id)

		pool, ok := e.shardPools[id]
		if ok {
			pool.Release()
			delete(e.shardPools, id)
		}

		e.log.Info("shard has been removed",
			zap.String("id", id))
	}
	e.mtx.Unlock()

	for _, sh := range ss {
		err := sh.Close()
		if err != nil {
			e.log.Error("could not close removed shard",
				zap.Stringer("id", sh.ID()),
				zap.Error(err),
			)
		}
	}
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
func (e *StorageEngine) SetShardMode(id *shard.ID, m mode.Mode, resetErrorCounter bool) error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	for shID, sh := range e.shards {
		if id.String() == shID {
			if resetErrorCounter {
				sh.errorCount.Store(0)
			}
			return sh.SetMode(m)
		}
	}

	return errShardNotFound
}

// HandleNewEpoch notifies every shard about NewEpoch event.
func (e *StorageEngine) HandleNewEpoch(epoch uint64) {
	ev := shard.EventNewEpoch(epoch)

	e.mtx.RLock()
	defer e.mtx.RUnlock()

	for _, sh := range e.shards {
		sh.NotificationChannel() <- ev
	}
}

func (s hashedShard) Hash() uint64 {
	return hrw.Hash(
		[]byte(s.Shard.ID().String()),
	)
}
