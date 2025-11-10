package engine

import (
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

var (
	errPutShard = errors.New("could not put object to any shard")

	errOverloaded = ants.ErrPoolOverload
	errExists     = errors.New("already exists")
)

// Put saves an object to local storage. objBin and hdrLen parameters are
// optional and used to optimize out object marshaling, when used both must
// be valid.
//
// Returns any error encountered that
// did not allow to completely save the object.
//
// Returns an error if executions are blocked (see BlockExecution).
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if the object has been marked as removed.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if obj is of [object.TypeLock]
// type and there is an object of [object.TypeTombstone] type associated with
// the same target.
func (e *StorageEngine) Put(obj *objectSDK.Object, objBin []byte) error {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddPutDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return e.blockErr
	}

	addr := object.AddressOf(obj)

	// In #1146 this check was parallelized, however, it became
	// much slower on fast machines for 4 shards.
	_, err := e.exists(addr)
	if err != nil {
		return err
	}

	// API 2.18+ system objects handling
	switch obj.Type() {
	case objectSDK.TypeTombstone:
		deleted := obj.AssociatedObject()
		if !deleted.IsZero() {
			tsExp, err := object.Expiration(*obj)
			if err != nil {
				return fmt.Errorf("cannot parse %s TS's expiration: %w", addr, err)
			}

			err = e.inhume([]oid.Address{oid.NewAddress(addr.Container(), deleted)}, false, &addr, tsExp)
			if err != nil {
				return fmt.Errorf("cannot inhume %s object on %s TS put: %w", deleted, addr, err)
			}
		}

		// Broadcast tombstone object to ALL shards to ensure availability everywhere
		return e.broadcastObject(obj, objBin)
	case objectSDK.TypeLock:
		locked := obj.AssociatedObject()
		if !locked.IsZero() {
			if err := e.lock(addr.Container(), addr.Object(), locked); err != nil {
				return err
			}
		}

		// Broadcast lock object to ALL shards to ensure availability everywhere
		return e.broadcastObject(obj, objBin)
	default:
	}

	var (
		bestPool   util.WorkerPool
		bestShard  shardWrapper
		overloaded bool
	)

	for i, sh := range e.sortedShards(addr) {
		e.mtx.RLock()
		pool, ok := e.shardPools[sh.ID().String()]
		if ok && bestPool == nil {
			bestShard = sh
			bestPool = pool
		}
		e.mtx.RUnlock()
		if !ok {
			// Shard was concurrently removed, skip.
			continue
		}

		err = e.putToShard(sh, i, pool, addr, obj, objBin)
		if err == nil || errors.Is(err, errExists) {
			return nil
		}
		if errors.Is(err, errOverloaded) {
			overloaded = true
		}
	}

	e.log.Debug("failed to put object to shards, trying the best one more",
		zap.Stringer("addr", addr), zap.Stringer("best shard", bestShard.ID()))

	if e.objectPutTimeout > 0 {
		success, over := e.putToShardWithDeadLine(bestShard, 0, bestPool, addr, obj, objBin)
		if success {
			return nil
		}
		if over {
			overloaded = true
		}
	}

	if overloaded {
		var busy = new(apistatus.Busy)
		busy.SetMessage(errPutShard.Error())
		return busy
	}

	return errPutShard
}

// putToShard puts object to sh.
// Returns error from shard put or errOverloaded (when shard pool can't accept
// the task) or errExists (if object is already stored there).
func (e *StorageEngine) putToShard(sh shardWrapper, ind int, pool util.WorkerPool, addr oid.Address, obj *objectSDK.Object, objBin []byte) error {
	var (
		alreadyExists bool
		err           error
		exitCh        = make(chan struct{})
		id            = sh.ID()
		putError      error
	)

	err = pool.Submit(func() {
		defer close(exitCh)

		exists, err := sh.Exists(addr, false)
		if err != nil {
			e.log.Warn("object put: check object existence",
				zap.Stringer("addr", addr),
				zap.Stringer("shard", id),
				zap.Error(err))

			if shard.IsErrObjectExpired(err) {
				// object is already found but
				// expired => do nothing with it
				alreadyExists = true
			}

			return // this is not ErrAlreadyRemoved error so we can go to the next shard
		}

		alreadyExists = exists
		if alreadyExists {
			if ind != 0 {
				err = sh.ToMoveIt(addr)
				if err != nil {
					e.log.Warn("could not mark object for shard relocation",
						zap.Stringer("shard", id),
						zap.Error(err),
					)
				}
			}

			e.log.Debug("object put: object already exists",
				zap.Stringer("shard", id),
				zap.Stringer("addr", addr))

			return
		}

		putError = sh.Put(obj, objBin)
		if putError != nil {
			if errors.Is(putError, shard.ErrReadOnlyMode) || errors.Is(putError, common.ErrReadOnly) ||
				errors.Is(putError, common.ErrNoSpace) {
				e.log.Warn("could not put object to shard",
					zap.Stringer("shard_id", id),
					zap.Error(putError))
				return
			}

			e.reportShardError(sh, "could not put object to shard", putError)
			return
		}
	})
	if err != nil {
		e.log.Warn("object put: pool task submitting", zap.Stringer("shard", id), zap.Error(err))
		close(exitCh)
		return err
	}

	<-exitCh

	if alreadyExists {
		return errExists
	}

	return putError
}

func (e *StorageEngine) putToShardWithDeadLine(sh shardWrapper, ind int, pool util.WorkerPool, addr oid.Address, obj *objectSDK.Object, objBin []byte) (bool, bool) {
	const putCooldown = 100 * time.Millisecond
	var (
		overloaded bool
		ticker     = time.NewTicker(putCooldown)
		timer      = time.NewTimer(e.objectPutTimeout)
	)

	for {
		select {
		case <-timer.C:
			e.log.Error("could not put object", zap.Stringer("addr", addr), zap.Duration("deadline", e.objectPutTimeout))
			return false, overloaded
		case <-ticker.C:
			err := e.putToShard(sh, ind, pool, addr, obj, objBin)
			if errors.Is(err, errOverloaded) {
				overloaded = true
				ticker.Reset(putCooldown)
				continue
			}

			return err == nil || errors.Is(err, errExists), false
		}
	}
}

// broadcastObject stores object on ALL shards to ensure it's available everywhere.
func (e *StorageEngine) broadcastObject(obj *objectSDK.Object, objBin []byte) error {
	var (
		successCount int
		pool         util.WorkerPool
		ok           bool
		allShards    = e.unsortedShards()
		addr         = object.AddressOf(obj)
		lastError    error
	)

	e.log.Debug("broadcasting object to all shards",
		zap.Stringer("type", obj.Type()),
		zap.Stringer("addr", addr),
		zap.Stringer("associated", obj.AssociatedObject()),
		zap.Int("shard_count", len(allShards)))

	for i, sh := range allShards {
		e.mtx.RLock()
		pool, ok = e.shardPools[sh.ID().String()]
		e.mtx.RUnlock()

		if !ok {
			// Shard was concurrently removed, skip.
			continue
		}

		err := e.putToShard(sh, i, pool, addr, obj, objBin)
		if err == nil || errors.Is(err, errExists) {
			successCount++
			if errors.Is(err, errExists) {
				e.log.Debug("object already exists on shard during broadcast",
					zap.Stringer("type", obj.Type()),
					zap.Stringer("associated", obj.AssociatedObject()),
					zap.Stringer("shard", sh.ID()),
					zap.Stringer("addr", addr))
			} else {
				e.log.Debug("successfully put object on shard during broadcast",
					zap.Stringer("type", obj.Type()),
					zap.Stringer("associated", obj.AssociatedObject()),
					zap.Stringer("shard", sh.ID()),
					zap.Stringer("addr", addr))
			}
			continue
		}
		lastError = err
		if errors.Is(err, apistatus.ErrLockNonRegularObject) ||
			errors.Is(err, apistatus.ErrObjectLocked) ||
			errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
			break
		}

		e.log.Warn("failed to put object on shard during broadcast",
			zap.Stringer("type", obj.Type()),
			zap.Stringer("shard", sh.ID()),
			zap.Stringer("addr", addr),
			zap.Stringer("associated", obj.AssociatedObject()),
			zap.Error(err))
	}

	e.log.Debug("object broadcast completed",
		zap.Stringer("type", obj.Type()),
		zap.Stringer("addr", addr),
		zap.Stringer("associated", obj.AssociatedObject()),
		zap.Int("success_count", successCount),
		zap.Int("total_shards", len(allShards)))

	if successCount == 0 {
		return fmt.Errorf("failed to broadcast %s object to any shard, last error: %w", obj.Type(), lastError)
	}

	return nil
}
