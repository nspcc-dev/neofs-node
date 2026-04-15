package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Head reads object header from local storage. If raw is true returns
// SplitInfo of the virtual object instead of the virtual object header.
//
// Returns any error encountered that
// did not allow to completely read the object header.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in local storage.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object was inhumed.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Head(addr oid.Address, raw bool) (*object.Object, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddHeadDuration)()
	}

	debugLogger := e.log.With(zap.String("component", "DEBUG TS panic, engine"), zap.Stringer("addr", addr))

	var res *object.Object
	return res, e.headFunc(addr, raw, func(sh *shard.Shard, addr oid.Address, raw bool) error {
		var err error
		res, err = sh.Head(addr, raw)

		debugLogger.Info("DEBUG: received object from shard", zap.Stringer("shardID", sh.ID()), zap.Bool("objIsNil", res == nil), zap.Error(err))

		return err
	})
}

// ReadHeader reads first bytes of the referenced object's binary containing its
// full header from e into buf. Returns number of bytes read.
//
// If object is missing, ReadHeader returns [apistatus.ErrObjectNotFound].
//
// If object is known but removed, ReadHeader returns
// [apistatus.ErrObjectAlreadyRemoved].
//
// If object is known but expired, ReadHeader returns
// [apistatus.ErrObjectNotFound].
//
// If object is a split-parent, behavior depends on raw flag. If set, ReadHeader
// returns [object.SplitInfoError] with all relations recorded in e. If unset,
// ReadHeader reads header of the requested object from the child one into buf.
func (e *StorageEngine) ReadHeader(addr oid.Address, raw bool, buf []byte) (int, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddReadHeaderDuration)()
	}

	var res int
	return res, e.headFunc(addr, raw, func(sh *shard.Shard, addr oid.Address, raw bool) error {
		var err error
		res, err = sh.ReadHeader(addr, raw, buf)
		return err
	})
}

func (e *StorageEngine) headFunc(addr oid.Address, raw bool, fn func(*shard.Shard, oid.Address, bool) error) error {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return e.blockErr
	}

	var splitInfo *object.SplitInfo

	for _, sh := range e.sortedShards(addr.Object()) {
		err := fn(sh.Shard, addr, raw)
		if err != nil {
			var siErr *object.SplitInfoError

			switch {
			case shard.IsErrNotFound(err):
				continue // ignore, go to next shard
			case errors.As(err, &siErr):
				if splitInfo == nil {
					splitInfo = object.NewSplitInfo()
				}

				util.MergeSplitInfo(siErr.SplitInfo(), splitInfo)

				// stop iterating over shards if SplitInfo structure is complete
				if !splitInfo.GetLink().IsZero() && !splitInfo.GetLastPart().IsZero() {
					return logicerr.Wrap(object.NewSplitInfoError(splitInfo))
				}
				continue
			case shard.IsErrRemoved(err):
				return err // stop, return it back
			case shard.IsErrObjectExpired(err):
				// object is found but should not
				// be returned
				return apistatus.ObjectNotFound{}
			default:
				e.reportShardError(sh, "could not head object from shard", err, zap.Stringer("addr", addr))
				continue
			}
		}

		return nil
	}

	if splitInfo != nil {
		return logicerr.Wrap(object.NewSplitInfoError(splitInfo))
	}

	return apistatus.ObjectNotFound{}
}
