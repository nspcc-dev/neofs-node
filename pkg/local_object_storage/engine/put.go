package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

var (
	errPutShard = errors.New("could not put object to any shard")
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
func (e *StorageEngine) Put(ctx context.Context, obj *object.Object, objBin []byte) error {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddPutDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	addr := obj.Address()

	err := e.preprocessPutLocked(addr)
	if err != nil {
		if errors.Is(err, ierrors.ErrObjectExists) {
			return nil
		}
		return err
	}

	// API 2.18+ system objects handling
	switch obj.Type() {
	case object.TypeTombstone, object.TypeLock, object.TypeLink:
		// Broadcast object to ALL shards to ensure availability everywhere.
		return e.broadcastObject(ctx, obj, objBin)
	default:
	}

	shs, err := e.sortShardsForPut(*obj)
	if err != nil {
		return err
	}

	for _, sh := range shs {
		err = e.putToShard(sh, addr, obj, objBin)
		if err == nil || errors.Is(err, ierrors.ErrObjectExists) {
			return nil
		}
	}

	return newPutAllShardsError(err)
}

func (e *StorageEngine) preprocessPutLocked(addr oid.Address) error {
	if e.blockErr != nil {
		return e.blockErr
	}

	// In #1146 this check was parallelized, however, it became
	// much slower on fast machines for 4 shards.
	exists, err := e.existsPhysical(addr)
	if err != nil {
		return err
	}

	if exists {
		return ierrors.ErrObjectExists
	}

	return nil
}

func (e *StorageEngine) sortShardsForPut(hdr object.Object) ([]shardWrapper, error) {
	var shs []shardWrapper
	if iec.ObjectWithAttributes(hdr) {
		shs = e.sortShardsFn(e, hdr.GetParentID())
	} else {
		shs = e.sortShardsFn(e, hdr.GetID())
	}
	if len(shs) == 0 {
		return nil, fmt.Errorf("%w: no shards", errPutShard)
	}
	return shs, nil
}

func (e *StorageEngine) checkExistsOnShard(sh shardWrapper, addr oid.Address) error {
	exists, err := sh.shardIface.Exists(addr, false)
	if err != nil {
		e.log.Warn("object put: check object existence",
			zap.Stringer("addr", addr),
			zap.Stringer("shard", sh.ID()),
			zap.Error(err))

		if shard.IsErrObjectExpired(err) {
			// object is already found but
			// expired => do nothing with it
			err = ierrors.ErrObjectExists
		}
		return err
	}

	if exists {
		return ierrors.ErrObjectExists
	}

	return nil
}

// putToShard puts object to sh.
// Returns error from shard put or errExists (if object is already stored there).
func (e *StorageEngine) putToShard(sh shardWrapper, addr oid.Address, obj *object.Object, objBin []byte) error {
	err := e.checkExistsOnShard(sh, addr)
	if err != nil {
		return err
	}

	err = sh.Put(obj, objBin)
	if err != nil {
		e.handleShardPutError(sh, err)
	}

	return err
}

func (e *StorageEngine) handleShardPutError(sh shardWrapper, err error) {
	if errors.Is(err, shard.ErrReadOnlyMode) || errors.Is(err, blobstor.ErrReadOnly) || errors.Is(err, blobstor.ErrNoSpace) {
		e.log.Warn("could not put object to shard",
			zap.Stringer("shard_id", sh.ID()), zap.Error(err))
		return
	}

	e.reportShardError(sh, "could not put object to shard", err)
}

// broadcastObject stores object on ALL shards to ensure it's available everywhere.
func (e *StorageEngine) broadcastObject(ctx context.Context, obj *object.Object, objBin []byte) error {
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
		if err == nil || errors.Is(err, ierrors.ErrObjectExists) {
			goodShards = append(goodShards, sh)
			if errors.Is(err, ierrors.ErrObjectExists) {
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

// InitPut calls [shard.Shard.InitPut] most optimal underlying shard for the
// object, switching to backup shards on error.
//
// The object must be of [object.TypeRegular] type, for others
// [StorageEngine.Put] should be used.
//
// If object already exists, InitPut returns [ierrors.ErrObjectExists].
func (e *StorageEngine) InitPut(_ context.Context, hdr object.Object, hdrLen uint64, hdrW io.WriterTo) (io.WriteCloser, func(), error) {
	if typ := hdr.Type(); typ != object.TypeRegular {
		return nil, nil, fmt.Errorf("invalid object type %s", typ)
	}

	var st time.Time
	if e.metrics != nil {
		st = time.Now()
	}

	addr := hdr.Address()

	e.blockMtx.RLock()

	var err error

	defer func() {
		e.blockMtx.RUnlock()
		if err != nil {
			e.submitFinishedInitPut(st)
		}
	}()

	err = e.preprocessPutLocked(addr)
	if err != nil {
		return nil, nil, err
	}

	shs, err := e.sortShardsForPut(hdr)
	if err != nil {
		return nil, nil, err
	}

	for _, sh := range shs {
		err = e.checkExistsOnShard(sh, addr)
		if err != nil {
			if errors.Is(err, ierrors.ErrObjectExists) {
				return nil, nil, ierrors.ErrObjectExists
			}
			continue
		}

		var stream io.WriteCloser
		var abortFn func()
		stream, abortFn, err = sh.shardIface.InitPut(hdr, hdrLen, hdrW)
		if err != nil {
			e.handleShardPutError(sh, err)
			continue
		}

		res := newPayloadWriteStream(e, sh, st, stream, abortFn)

		return res, res.abort, err
	}

	return nil, nil, newPutAllShardsError(err)
}

func (e *StorageEngine) submitFinishedInitPut(st time.Time) {
	if e.metrics != nil {
		e.metrics.AddStreamingPutDuration(time.Since(st))
	}
}

type payloadWriteStream struct {
	storageEngine *StorageEngine
	shard         shardWrapper
	startTime     time.Time
	stream        io.WriteCloser
	abortFn       func()
	aborted       bool
}

func newPayloadWriteStream(storageEngine *StorageEngine, sh shardWrapper, st time.Time, stream io.WriteCloser, abortFn func()) *payloadWriteStream {
	return &payloadWriteStream{
		storageEngine: storageEngine,
		shard:         sh,
		startTime:     st,
		stream:        stream,
		abortFn:       abortFn,
	}
}

func (x *payloadWriteStream) Write(p []byte) (int, error) {
	if x.aborted {
		return 0, logicerr.ErrStreamAborted
	}

	n, err := x.stream.Write(p)
	if err != nil {
		x.storageEngine.handleShardPutError(x.shard, err)
		x.finish()
		return n, newPutAllShardsError(err)
	}

	return n, nil
}

func (x *payloadWriteStream) Close() error {
	if x.aborted {
		return logicerr.ErrStreamAborted
	}

	defer x.finish()

	err := x.stream.Close()
	if err != nil {
		x.storageEngine.handleShardPutError(x.shard, err)
		return newPutAllShardsError(err)
	}

	return nil
}

func (x *payloadWriteStream) abort() {
	if x.aborted {
		return
	}
	x.abortFn()
	x.finish()
}

func (x *payloadWriteStream) finish() {
	x.storageEngine.submitFinishedInitPut(x.startTime)
	x.aborted = true
}

func newPutAllShardsError(err error) error {
	return fmt.Errorf("%w: %w", errPutShard, err)
}
