package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"

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

	errExists = errors.New("already exists")
)

func (e *StorageEngine) precheckPutLocked(hdr object.Object) error {
	if e.blockErr != nil {
		return e.blockErr
	}

	addr := hdr.Address()

	// In #1146 this check was parallelized, however, it became
	// much slower on fast machines for 4 shards.
	exists, err := e.existsPhysical(addr)
	if err != nil {
		return err
	}
	if exists {
		return fs.ErrExist
	}

	return nil
}

func isBroadcastObject(hdr object.Object) bool {
	// API 2.18+ system objects handling
	switch hdr.Type() {
	case object.TypeTombstone, object.TypeLock, object.TypeLink:
		// Broadcast object to ALL shards to ensure availability everywhere.
		return true
	default:
		return false
	}
}

func (e *StorageEngine) sortShardsForObject(hdr object.Object) ([]shardWrapper, error) {
	var shs []shardWrapper
	if iec.ObjectWithAttributes(hdr) {
		shs = e.sortedShards(hdr.GetParentID())
	} else {
		shs = e.sortedShards(hdr.GetID())
	}

	if len(shs) == 0 {
		return nil, fmt.Errorf("%w: no shards", errPutShard)
	}

	return shs, nil
}

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

	err := e.precheckPutLocked(*obj)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil
		}
		return err
	}

	if isBroadcastObject(*obj) {
		return e.broadcastObject(ctx, obj, objBin)
	}

	shs, err := e.sortShardsForObject(*obj)
	if err != nil {
		return err
	}

	for _, sh := range shs {
		err = e.putToShard(sh, addr, obj, objBin)
		if err == nil || errors.Is(err, errExists) {
			return nil
		}
	}

	return fmt.Errorf("%w: %w", errPutShard, err)
}

// putToShard puts object to sh.
// Returns error from shard put or errExists (if object is already stored there).
func (e *StorageEngine) putToShard(sh shardWrapper, addr oid.Address, obj *object.Object, objBin []byte) error {
	return e.putToShardFunc(sh, addr, func(sh *shard.Shard) error {
		return sh.Put(obj, objBin)
	})
}

func (e *StorageEngine) putToShardFunc(sh shardWrapper, addr oid.Address, putFn func(*shard.Shard) error) error {
	exists, err := sh.Exists(addr, false)
	if err != nil {
		sh.engine.log.Warn("object put: check object existence",
			zap.Stringer("addr", addr),
			zap.Stringer("shard", sh.ID()),
			zap.Error(err))

		if shard.IsErrObjectExpired(err) {
			// object is already found but
			// expired => do nothing with it
			err = errExists
		}
		return err
	}

	if exists {
		return errExists
	}

	err = putFn(sh.Shard)
	if err != nil {
		if errors.Is(err, shard.ErrReadOnlyMode) || errors.Is(err, common.ErrReadOnly) ||
			errors.Is(err, common.ErrNoSpace) {
			sh.engine.log.Warn("could not put object to shard",
				zap.Stringer("shard_id", sh.ID()),
				zap.Error(err))
		} else {
			sh.engine.reportShardError(sh, "could not put object to shard", err)
		}
	}

	return err
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

// TODO: docs.
func (e *StorageEngine) InitPut(_ context.Context, hdr object.Object) (io.WriteCloser, func(), error) {
	// TODO: metric?
	// if e.metrics != nil {
	// 	defer elapsed(e.metrics.AddPutDuration)()
	// }

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	err := e.precheckPutLocked(hdr)
	if err != nil {
		return nil, nil, err
	}

	if isBroadcastObject(hdr) {
		hdr.SetPayload(slices.Clip(hdr.Payload()))
		bw := broadcastingWriter{
			engine: e,
			object: hdr,
		}
		return &bw, func() {}, nil
	}

	shs, err := e.sortShardsForObject(hdr)
	if err != nil {
		return nil, nil, err
	}

	var payloadStream io.WriteCloser
	var abortFn func()

	for i := range shs {
		err = e.putToShardFunc(shs[i], hdr.Address(), func(sh *shard.Shard) error {
			var err error
			payloadStream, abortFn, err = sh.InitPut(hdr)
			return err
		})
		if err == nil {
			return payloadStream, abortFn, nil
		}
		if errors.Is(err, errExists) {
			return nil, nil, fs.ErrExist
		}
	}

	return nil, nil, fmt.Errorf("%w: %w", errPutShard, err)
}

type broadcastingWriter struct {
	engine *StorageEngine
	object object.Object
}

func (x *broadcastingWriter) Write(p []byte) (int, error) {
	x.object.SetPayload(append(x.object.Payload(), p...))
	return len(p), nil
}

func (x *broadcastingWriter) Close() error {
	return x.engine.broadcastObject(context.Background(), &x.object, nil)
}
