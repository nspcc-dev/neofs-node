package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

var (
	errPutShard = errors.New("could not put object to any shard")

	errOverloaded = errors.New("overloaded")
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
func (e *StorageEngine) Put(obj *object.Object, objBin []byte) error {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddPutDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return e.blockErr
	}

	addr := obj.Address()

	// In #1146 this check was parallelized, however, it became
	// much slower on fast machines for 4 shards.
	exists, err := e.existsPhysical(addr)
	if err != nil || exists {
		return err
	}

	// API 2.18+ system objects handling
	switch obj.Type() {
	case object.TypeTombstone, object.TypeLock, object.TypeLink:
		// Broadcast object to ALL shards to ensure availability everywhere.
		return e.broadcastObject(obj, objBin)
	default:
	}

	var (
		overloaded bool
		shs        []shardWrapper
	)

	if iec.ObjectWithAttributes(*obj) {
		shs = e.sortedShards(obj.GetParentID())
	} else {
		shs = e.sortedShards(addr.Object())
	}

	if len(shs) == 0 {
		return fmt.Errorf("%w: no shards", errPutShard)
	}

	for _, sh := range shs {
		err = e.putToShard(sh, addr, obj, objBin)
		if err == nil || errors.Is(err, errExists) {
			return nil
		}
		if errors.Is(err, errOverloaded) {
			overloaded = true
		}
	}

	if overloaded {
		var busy = new(apistatus.Busy)
		busy.SetMessage(errPutShard.Error())
		return busy
	}

	return fmt.Errorf("%w: %w", errPutShard, err)
}

// putToShard puts object to sh.
// Returns error from shard put or errOverloaded (when shard pool can't accept
// the task) or errExists (if object is already stored there).
func (e *StorageEngine) putToShard(sh shardWrapper, addr oid.Address, obj *object.Object, objBin []byte) error {
	var (
		exitCh      = make(chan error)
		ctx, cancel = context.WithTimeout(context.TODO(), e.objectPutTimeout+time.Millisecond) // 1ms to avoid zero value.
	)
	defer cancel()

	select {
	case sh.putCh <- putTask{addr: addr, obj: obj, objBin: objBin, retCh: exitCh}:
	case <-ctx.Done():
		return errOverloaded
	}

	err := <-exitCh
	return err
}

func (sh *shardWrapper) shardPutThread() {
	var id = sh.ID().String()

	for t := range sh.putCh {
		exists, err := sh.Exists(t.addr, false)
		if err != nil {
			sh.engine.log.Warn("object put: check object existence",
				zap.Stringer("addr", t.addr),
				zap.String("shard", id),
				zap.Error(err))

			if shard.IsErrObjectExpired(err) {
				// object is already found but
				// expired => do nothing with it
				err = errExists
			}
			t.retCh <- err
			continue // this is not ErrAlreadyRemoved error so we can go to the next task
		}

		if exists {
			t.retCh <- errExists
			continue
		}

		err = sh.Put(t.obj, t.objBin)
		if err != nil {
			if errors.Is(err, shard.ErrReadOnlyMode) || errors.Is(err, common.ErrReadOnly) ||
				errors.Is(err, common.ErrNoSpace) {
				sh.engine.log.Warn("could not put object to shard",
					zap.String("shard_id", id),
					zap.Error(err))
			} else {
				sh.engine.reportShardError(*sh, "could not put object to shard", err)
			}
		}
		t.retCh <- err
	}
}

// broadcastObject stores object on ALL shards to ensure it's available everywhere.
func (e *StorageEngine) broadcastObject(obj *object.Object, objBin []byte) error {
	var (
		allShards  = e.unsortedShards()
		addr       = obj.Address()
		goodShards = make([]shardWrapper, 0, len(allShards))
		lastError  error
		isFatal    bool
	)

	e.log.Debug("broadcasting object to all shards",
		zap.Stringer("type", obj.Type()),
		zap.Stringer("addr", addr),
		zap.Stringer("associated", obj.AssociatedObject()),
		zap.Int("shard_count", len(allShards)))

	for _, sh := range allShards {
		err := e.putToShard(sh, addr, obj, objBin)
		if err == nil || errors.Is(err, errExists) {
			goodShards = append(goodShards, sh)
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
			isFatal = true
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
		zap.Error(lastError),
		zap.Bool("isFatal", isFatal),
		zap.Int("success_count", len(goodShards)),
		zap.Int("total_shards", len(allShards)))

	if isFatal && len(goodShards) > 0 {
		// Revert potential damage.
		for _, sh := range goodShards {
			var err = sh.Delete(addr.Container(), []oid.ID{addr.Object()})
			if err != nil {
				e.log.Warn("failed to rollback incorrect put",
					zap.Stringer("shard", sh.ID()),
					zap.Stringer("addr", addr),
					zap.Error(err))
			}
		}
	}

	if isFatal || len(goodShards) == 0 {
		return fmt.Errorf("failed to broadcast %s object to any shard, last error: %w", obj.Type(), lastError)
	}

	return nil
}
