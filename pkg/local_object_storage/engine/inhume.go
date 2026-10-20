package engine

import (
	"context"
	"errors"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

var errInhumeFailure = errors.New("inhume operation failed")

// InhumeContainer marks every object in a container as removed.
// Any further [StorageEngine.Get] calls will return [apistatus.ObjectNotFound]
// errors.
// There is no any LOCKs, forced GC marks and any relations checks,
// every object that belongs to a provided container will be marked
// as a removed one.
func (e *StorageEngine) InhumeContainer(_ context.Context, cID cid.ID) error {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return e.blockErr
	}
	for _, sh := range e.unsortedShards() {
		err := sh.InhumeContainer(cID)
		if err != nil {
			e.log.Warn("inhuming container",
				zap.Stringer("cid", cID),
				zap.Stringer("shard", sh.ID()),
				zap.Error(err))
		}
	}

	return nil
}

// processAddrDelete processes deletion (inhume or immediate delete) of an object by its address.
func (e *StorageEngine) processAddrDelete(addr oid.Address, deleteFunc func(*shard.Shard, cid.ID, []oid.ID) error) error {
	return e.processAddrDeleteOnShards(e.sortedShards(addr.Object()), addr, deleteFunc)
}

func (e *StorageEngine) processAddrDeleteOnShards(shards []shardWrapper, addr oid.Address, deleteFunc func(*shard.Shard, cid.ID, []oid.ID) error) error {
	var (
		children []oid.ID
		err      error
		root     bool
		siNoLink *object.SplitInfo
	)

	// see if the object is root
	for _, sh := range shards {
		exists, err := sh.Exists(addr, true)
		if err != nil {
			if errors.Is(err, apistatus.ErrObjectNotFound) {
				continue
			}

			if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
				// inhumed once - no need to be inhumed again
				return nil
			}

			if _, ok := errors.AsType[iec.ErrParts](err); ok {
				// TODO: Boolean switch, we don't need errECParts elements. Support and do fast return from Exists().
				root = true
				break
			}

			siErr, isSI := errors.AsType[*object.SplitInfoError](err)
			if !isSI {
				e.reportShardError(sh, "could not check for presence in shard", err, zap.Stringer("addr", addr))
				continue
			}

			root = true

			// object is root; every container node is expected to store
			// link object and link object existence (root object upload
			// has been finished) should be ensured on the upper levels
			linkID := siErr.SplitInfo().GetLink()
			if linkID.IsZero() {
				siNoLink = siErr.SplitInfo()
				// keep searching for the link object
				continue
			}

			siNoLink = nil

			var linkAddr oid.Address
			linkAddr.SetContainer(addr.Container())
			linkAddr.SetObject(linkID)

			linkObj, err := e.getInt(linkAddr)
			if err != nil {
				e.log.Debug("inhuming root object but no link object is found",
					zap.Stringer("linkAddr", linkAddr),
					zap.Stringer("addrBeingInhumed", addr),
					zap.Error(err))

				// link object not found, but we still need to try to delete the root object
				siNoLink = siErr.SplitInfo()
				break
			}

			// v2 split
			if linkObj.Type() == object.TypeLink {
				var link object.Link
				err := linkObj.ReadLink(&link)
				if err != nil {
					e.log.Debug("inhuming root object but link object cannot be read",
						zap.Stringer("linkAddr", linkAddr),
						zap.Stringer("addrBeingInhumed", addr),
						zap.Error(err))

					// link object cannot be read, but we still need to try to delete the root object
					siNoLink = siErr.SplitInfo()
					break
				}

				children = measuredObjsToOIDs(addr.Container(), link.Objects())
			} else {
				// v1 split
				children = linkObj.Children()
			}

			children = append(children, linkID)

			break
		}

		if !exists {
			continue
		}

		err = deleteFunc(sh.Shard, addr.Container(), []oid.ID{addr.Object()})
		if err != nil {
			if !errors.Is(err, logicerr.Error) {
				e.reportShardError(sh, "could not inhume object in shard", err, zap.Stringer("addr", addr))
			}
			return err
		}
	}

	if !root {
		return nil // Already deleted everywhere.
	}

	if siNoLink != nil {
		children = e.collectChildrenWithoutLink(addr, siNoLink)
	}

	var (
		addrs  = append(children, addr.Object())
		ok     bool
		retErr error
	)

	// has not found the object on any shard, so delete on the most probable one
	for _, sh := range shards {
		err = deleteFunc(sh.Shard, addr.Container(), addrs)
		if err != nil {
			var errLocked apistatus.ObjectLocked

			switch {
			case errors.As(err, &errLocked):
				return apistatus.ObjectLocked{} // Always a final error if returned.
			case errors.Is(err, shard.ErrLockObjectRemoval):
				return meta.ErrLockObjectRemoval // Always a final error if returned.
			case errors.Is(err, shard.ErrReadOnlyMode) || errors.Is(err, shard.ErrDegradedMode):
				retErr = err
				continue
			}

			e.reportShardError(sh, "could not inhume object in shard", err, zap.Stringer("addr", addr))
			continue
		}

		ok = true
	}

	if retErr == nil && !ok {
		retErr = errInhumeFailure
	}
	return retErr
}

func (e *StorageEngine) collectChildrenWithoutLink(addr oid.Address, si *object.SplitInfo) []oid.ID {
	e.log.Info("root object has no link object in split upload",
		zap.Stringer("addrBeingInhumed", addr))

	firstID := si.GetFirstPart()
	splitID := si.SplitID()
	switch {
	case !firstID.IsZero():
		res, err := e.collectRawWithAttribute(addr.Container(), object.FilterFirstSplitObject, firstID[:])
		if err == nil {
			res = append(res, firstID)
			return res
		}
		e.log.Warn("failed to collect objects with first ID", zap.Stringer("addrBeingInhumed", addr), zap.Error(err))
	case splitID != nil:
		res, err := e.collectRawWithAttribute(addr.Container(), object.FilterSplitID, splitID.ToV2())
		if err == nil {
			return res
		}
		e.log.Warn("failed to collect objects with split ID", zap.Stringer("addrBeingInhumed", addr), zap.Error(err))
	default:
		e.log.Warn("no first ID and split ID found in split",
			zap.Stringer("addrBeingInhumed", addr))
	}

	return nil
}

// IsLocked checks whether an object is locked according to StorageEngine's state.
func (e *StorageEngine) IsLocked(_ context.Context, addr oid.Address) (bool, error) {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return false, e.blockErr
	}

	lockID, _, err := e.getLock(addr)
	return !lockID.IsZero(), err
}

// getLock returns an active LOCK's ID and the shard that stores it.
// Returns a zero ID and nil shard if addr is not locked.
func (e *StorageEngine) getLock(addr oid.Address) (oid.ID, *shard.Shard, error) {
	for _, sh := range e.unsortedShards() {
		lockID, err := sh.GetLockID(addr)
		if err != nil {
			e.reportShardError(sh, "can't check object's lockers", err, zap.Stringer("addr", addr))
			return oid.ID{}, nil, err
		}

		if !lockID.IsZero() {
			return lockID, sh.Shard, nil
		}
	}

	return oid.ID{}, nil, nil
}

func (e *StorageEngine) processExpiredObjects(addrs []oid.Address) {
	var rLocked = e.blockMtx.TryRLock()

	if !rLocked || e.blockErr != nil {
		return
	}
	defer e.blockMtx.RUnlock()

	for _, addr := range addrs {
		lockID, source, err := e.getLock(addr)
		if err != nil {
			e.log.Warn("removing an object without full locking check",
				zap.Error(err),
				zap.Stringer("addr", addr))
		} else if !lockID.IsZero() {
			e.log.Warn("skip an expired object with lock",
				zap.Stringer("addr", addr))
			e.restoreLock(source, oid.NewAddress(addr.Container(), lockID))
			continue
		}

		err = e.processAddrDelete(addr, (*shard.Shard).Delete)
		if err != nil {
			e.log.Warn("deleting expired object", zap.Stringer("addr", addr), zap.Error(err))
		}
	}
}

// restoreLock copies one active LOCK to shards that missed its broadcast.
// Copy failures must not roll back existing LOCKs.
func (e *StorageEngine) restoreLock(source *shard.Shard, lockAddr oid.Address) {
	lock, err := source.Get(lockAddr, false)
	if err != nil {
		e.log.Warn("could not read LOCK to restore", zap.Stringer("addr", lockAddr), zap.Error(err))
		return
	}

	for _, target := range e.unsortedShards() {
		if target.GetMode() != mode.ReadWrite {
			continue
		}
		err = e.putToShard(target, lockAddr, lock, nil)
		if err != nil && !errors.Is(err, ierrors.ErrObjectExists) {
			e.log.Warn("could not restore LOCK",
				zap.Stringer("addr", lockAddr),
				zap.Stringer("shard", target.ID()), zap.Error(err))
		}
	}
}

func measuredObjsToOIDs(cID cid.ID, mm []object.MeasuredObject) []oid.ID {
	res := make([]oid.ID, 0, len(mm))
	for i := range mm {
		res = append(res, mm[i].ObjectID())
	}

	return res
}
