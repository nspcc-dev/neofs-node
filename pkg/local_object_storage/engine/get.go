package engine

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Get reads an object from local storage.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in local storage.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the object has been marked as removed.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Get(addr oid.Address) (*objectSDK.Object, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddGetDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	var (
		err error
		obj *objectSDK.Object
	)

	err = e.get(addr, func(s *shard.Shard, ignoreMetadata bool) error {
		obj, err = s.Get(addr, ignoreMetadata)
		return err
	})
	return obj, err
}

func (e *StorageEngine) get(addr oid.Address, shardFunc func(s *shard.Shard, ignoreMetadata bool) error) error {
	var (
		hasDegraded   bool
		shardWithMeta shardWrapper
		splitInfo     *objectSDK.SplitInfo
		metaError     error
	)

	for _, sh := range e.sortedShards(addr) {
		noMeta := sh.GetMode().NoMetabase()
		hasDegraded = hasDegraded || noMeta

		err := shardFunc(sh.Shard, noMeta)
		if err != nil {
			var siErr *objectSDK.SplitInfoError

			if errors.Is(err, shard.ErrMetaWithNoObject) {
				shardWithMeta = sh
				metaError = err
			}
			switch {
			case shard.IsErrNotFound(err):
				continue // ignore, go to next shard
			case errors.As(err, &siErr):
				if splitInfo == nil {
					splitInfo = objectSDK.NewSplitInfo()
				}

				util.MergeSplitInfo(siErr.SplitInfo(), splitInfo)

				// stop iterating over shards if SplitInfo structure is complete
				if !splitInfo.GetLink().IsZero() && !splitInfo.GetLastPart().IsZero() {
					return logicerr.Wrap(objectSDK.NewSplitInfoError(splitInfo))
				}
				continue
			case
				shard.IsErrRemoved(err),
				shard.IsErrOutOfRange(err):
				return err // stop, return it back
			case shard.IsErrObjectExpired(err):
				// object is found but should not
				// be returned
				return apistatus.ObjectNotFound{}
			default:
				e.reportShardError(sh, "could not get object from shard", err)
				continue
			}
		}

		return nil // shardFunc is successful and it has the result
	}

	if splitInfo != nil {
		return logicerr.Wrap(objectSDK.NewSplitInfoError(splitInfo))
	}

	if !hasDegraded && shardWithMeta.Shard == nil {
		return apistatus.ObjectNotFound{}
	}

	// If the object is not found but is present in metabase,
	// try to fetch it from blobstor directly. If it is found in any
	// blobstor, increase the error counter for the shard which contains the meta.
	for _, sh := range e.sortedShards(addr) {
		if sh.GetMode().NoMetabase() {
			// Already visited.
			continue
		}

		err := shardFunc(sh.Shard, true)
		if shard.IsErrOutOfRange(err) {
			return apistatus.ObjectOutOfRange{}
		}
		if err == nil {
			if shardWithMeta.Shard != nil {
				e.reportShardError(shardWithMeta, "meta info was present, but object is missing",
					metaError, zap.Stringer("address", addr))
			}
			return nil
		}
	}
	return apistatus.ObjectNotFound{}
}

// GetBytes reads object from the StorageEngine by address into memory buffer in
// a canonical NeoFS binary format. Returns [apistatus.ObjectNotFound] if object
// is missing.
func (e *StorageEngine) GetBytes(addr oid.Address) ([]byte, error) {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	var (
		b   []byte
		err error
	)
	err = e.get(addr, func(s *shard.Shard, ignoreMetadata bool) error {
		if ignoreMetadata {
			b, err = s.GetBytes(addr)
		} else {
			b, err = s.GetBytesWithMetadataLookup(addr)
		}
		return err
	})
	return b, err
}

// TODO:: docs.
// TODO: keep in sync with https://github.com/nspcc-dev/neofs-node/pull/3466.
func (e *StorageEngine) GetECPartByIdx(cnr cid.ID, parent oid.ID, idx int) (objectSDK.Object, error) {
	// TODO: metrics and blockErr like Get

	// TODO: sync placement with PUT. They must sort shard equally
	shs := e.sortedShards(oid.NewAddress(cnr, parent))
	for i := range shs {
		obj, err := shs[i].GetECPartByIdx(cnr, parent, idx)
		if err == nil {
			return obj, nil
		}
		e.log.Info("failed to get EC part from shard", zap.Stringer("container", cnr), zap.Stringer("parent", parent),
			zap.Int("idx", idx), zap.Error(err))
		// FIXME: some errors like 'akready removed' must abort
	}

	return objectSDK.Object{}, fmt.Errorf("%w: all shards failed", apistatus.ErrObjectNotFound)
}

// TODO:: docs.
// TODO: keep in sync with https://github.com/nspcc-dev/neofs-node/pull/3466.
func (e *StorageEngine) GetAnyECPart(cnr cid.ID, parent oid.ID) (objectSDK.Object, error) {
	// TODO: metrics and blockErr like Get

	// TODO: sync placement with PUT. They must sort shard equally
	shs := e.sortedShards(oid.NewAddress(cnr, parent))
	for i := range shs {
		obj, err := shs[i].GetAnyECPart(cnr, parent)
		if err == nil {
			return obj, nil
		}
		e.log.Info("failed to get EC part from shard", zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Error(err))
		// FIXME: some errors like 'akready removed' must abort
	}

	return objectSDK.Object{}, fmt.Errorf("%w: all shards failed", apistatus.ErrObjectNotFound)
}
