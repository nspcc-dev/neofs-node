package engine

import (
	"context"
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// OptimizeShardLocation moves an object to its most preferred shard according
// to HRW ordering and marks its copies on all other listed shards as redundant.
//
// The source copies are marked only after the object is known to exist on the
// preferred shard. The operation therefore never removes the last local copy
// when the destination cannot accept the object.
//
// It returns true if it copied the object to the preferred shard. It returns
// false when the object was already there.
//
// Returns an error if executions are blocked (see BlockExecution), no listed
// source shard contains the object, or optimization/marking fails.
func (e *StorageEngine) OptimizeShardLocation(_ context.Context, addr oid.Address, shardIDs []string) (bool, error) {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return false, e.blockErr
	}
	if len(shardIDs) == 0 {
		return false, errShardNotFound
	}

	shards := e.sortedShards(addr.Object())
	if len(shards) == 0 {
		return false, errShardNotFound
	}

	target := shards[0]
	targetID := target.ID().String()
	if len(shardIDs) == 1 && shardIDs[0] == targetID {
		return false, nil
	}

	exists, err := target.Exists(addr, false)
	if err != nil && !errors.Is(err, apistatus.ErrObjectNotFound) {
		return false, err
	}

	moved := false
	if !exists {
		var (
			obj           *object.Object
			sourceShardID string
		)
		for _, sourceID := range shardIDs {
			if sourceID == targetID {
				continue
			}

			source := e.getShard(sourceID)
			if source.Shard == nil {
				continue
			}

			obj, err = source.Get(addr, false)
			if err == nil {
				sourceShardID = sourceID
				break
			}
			if !errors.Is(err, apistatus.ErrObjectNotFound) {
				return false, err
			}
		}
		if obj == nil {
			return false, apistatus.ObjectNotFound{}
		}

		err = e.putToShard(target, addr, obj, nil)
		if err != nil && !errors.Is(err, errExists) {
			return false, err
		}
		moved = err == nil
		if moved {
			e.log.Info("moved local object to preferred shard",
				zap.Stringer("object", addr),
				zap.String("source_shard", sourceShardID),
				zap.String("target_shard", targetID))
		}
	}

	for _, sourceID := range shardIDs {
		if sourceID == targetID {
			continue
		}

		source := e.getShard(sourceID)
		if source.Shard == nil {
			continue
		}

		if err = source.MarkGarbage(addr.Container(), []oid.ID{addr.Object()}, GarbageMarkRedundant); err != nil {
			return false, err
		}
		e.log.Info("marked redundant local object copy",
			zap.Stringer("object", addr),
			zap.String("keeper_shard", targetID),
			zap.String("redundant_shard", sourceID))
	}

	return moved, nil
}
