package engine

import (
	"errors"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

var errInhumeFailure = errors.New("inhume operation failed")

// Inhume calls [metabase.Inhume] method to mark an object as removed following
// tombstone data. It won't be removed physically from the shard until GC cycle
// does it.
//
// Allows inhuming non-locked objects only. Returns apistatus.ObjectLocked
// if at least one object is locked.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Inhume(tombstone oid.Address, tombExpiration uint64, addrs ...oid.Address) error {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddInhumeDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return e.blockErr
	}
	return e.inhume(addrs, false, &tombstone, tombExpiration)
}

func (e *StorageEngine) inhume(addrs []oid.Address, force bool, tombstone *oid.Address, tombExpiration uint64) error {
	var shPrm shard.InhumePrm
	if force {
		shPrm.ForceRemoval()
	}

	for i := range addrs {
		if !force {
			locked, err := e.IsLocked(addrs[i])
			if err != nil {
				e.log.Warn("removing an object without full locking check",
					zap.Error(err),
					zap.Stringer("addr", addrs[i]))
			} else if locked {
				var lockedErr apistatus.ObjectLocked
				return lockedErr
			}
		}

		if tombstone != nil {
			shPrm.InhumeByTomb(*tombstone, tombExpiration, addrs[i])
		} else {
			shPrm.MarkAsGarbage(addrs[i])
		}

		ok, err := e.inhumeAddr(addrs[i], shPrm)
		if err != nil {
			return err
		}
		if !ok {
			return errInhumeFailure
		}
	}

	return nil
}

// InhumeContainer marks every object in a container as removed.
// Any further [StorageEngine.Get] calls will return [apistatus.ObjectNotFound]
// errors.
// There is no any LOCKs, forced GC marks and any relations checks,
// every object that belongs to a provided container will be marked
// as a removed one.
func (e *StorageEngine) InhumeContainer(cID cid.ID) error {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return e.blockErr
	}
	for _, sh := range e.unsortedShards() {
		err := sh.InhumeContainer(cID)
		if err != nil {
			e.log.Warn("inhuming container",
				zap.Stringer("shard", sh.ID()),
				zap.Error(err))
		}
	}

	return nil
}

// Returns ok if object was inhumed during this invocation or before.
func (e *StorageEngine) inhumeAddr(addr oid.Address, prm shard.InhumePrm) (bool, error) {
	var shardWithObject string

	var root bool
	var children []oid.Address

	// see if the object is root
	for _, sh := range e.unsortedShards() {
		exists, err := sh.Exists(addr, true)
		if err != nil {
			if shard.IsErrNotFound(err) {
				continue
			}

			if shard.IsErrRemoved(err) || shard.IsErrObjectExpired(err) {
				// inhumed once - no need to be inhumed again
				return true, nil
			}

			var siErr *objectSDK.SplitInfoError
			if !errors.As(err, &siErr) {
				e.reportShardError(sh, "could not check for presence in shard", err, zap.Stringer("addr", addr))
				continue
			}

			root = true

			// object is root; every container node is expected to store
			// link object and link object existence (root object upload
			// has been finished) should be ensured on the upper levels
			linkID := siErr.SplitInfo().GetLink()
			if linkID.IsZero() {
				// keep searching for the link object
				continue
			}

			var linkAddr oid.Address
			linkAddr.SetContainer(addr.Container())
			linkAddr.SetObject(linkID)

			linkObj, err := e.Get(linkAddr)
			if err != nil {
				e.log.Error("inhuming root object but no link object is found", zap.Error(err))

				// nothing can be done here, so just returning ok
				// to continue handling other addresses
				return true, nil
			}

			// v2 split
			if linkObj.Type() == objectSDK.TypeLink {
				var link objectSDK.Link
				err := linkObj.ReadLink(&link)
				if err != nil {
					e.log.Error("inhuming root object but link object cannot be read", zap.Error(err))

					// nothing can be done here, so just returning ok
					// to continue handling other addresses
					return true, nil
				}

				children = measuredObjsToAddresses(addr.Container(), link.Objects())
			} else {
				// v1 split
				children = oIDsToAddresses(addr.Container(), linkObj.Children())
			}

			children = append(children, linkAddr)

			break
		}

		if exists {
			shardWithObject = sh.ID().String()
			break
		}
	}

	prm.SetTargets(append(children, addr)...)

	if shardWithObject != "" {
		sh := e.getShard(shardWithObject)

		_, err := sh.Inhume(prm)
		if err != nil {
			if !errors.Is(err, logicerr.Error) {
				e.reportShardError(sh, "could not inhume object in shard", err)
			}

			return false, err
		}

		return true, nil
	}

	var (
		ok     bool
		retErr error
	)

	// has not found the object on any shard, so mark it as inhumed on the most probable one
	for _, sh := range e.sortedShards(addr) {
		_, err := sh.Inhume(prm)
		if err != nil {
			var errLocked apistatus.ObjectLocked

			switch {
			case errors.As(err, &errLocked):
				return false, apistatus.ObjectLocked{} // Always a final error if returned.
			case errors.Is(err, shard.ErrLockObjectRemoval):
				return false, meta.ErrLockObjectRemoval // Always a final error if returned.
			case errors.Is(err, shard.ErrReadOnlyMode) || errors.Is(err, shard.ErrDegradedMode):
				if root {
					retErr = err
					continue
				}
				return false, err
			}

			e.reportShardError(sh, "could not inhume object in shard", err)
			continue
		}

		ok = true
		if !root {
			break
		}
	}

	return ok, retErr
}

// IsLocked checks whether an object is locked according to StorageEngine's state.
func (e *StorageEngine) IsLocked(addr oid.Address) (bool, error) {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return false, e.blockErr
	}

	for _, sh := range e.unsortedShards() {
		locked, err := sh.Shard.IsLocked(addr)
		if err != nil {
			e.reportShardError(sh, "can't check object's lockers", err, zap.Stringer("addr", addr))
			return false, err
		}

		if locked {
			return true, nil
		}
	}

	return false, nil
}

func (e *StorageEngine) processExpiredObjects(addrs []oid.Address) {
	err := e.inhume(addrs, false, nil, 0)
	if err != nil {
		e.log.Warn("handling expired objects", zap.Error(err))
	}
}

func (e *StorageEngine) processExpiredLocks(lockers []oid.Address) {
	for _, sh := range e.unsortedShards() {
		sh.HandleExpiredLocks(lockers)
	}
}

func (e *StorageEngine) processDeletedLocks(lockers []oid.Address) {
	for _, sh := range e.unsortedShards() {
		sh.HandleDeletedLocks(lockers)
	}
}

func measuredObjsToAddresses(cID cid.ID, mm []objectSDK.MeasuredObject) []oid.Address {
	var addr oid.Address
	addr.SetContainer(cID)

	res := make([]oid.Address, 0, len(mm))
	for i := range mm {
		addr.SetObject(mm[i].ObjectID())
		res = append(res, addr)
	}

	return res
}

func oIDsToAddresses(cID cid.ID, oo []oid.ID) []oid.Address {
	var addr oid.Address
	addr.SetContainer(cID)

	res := make([]oid.Address, 0, len(oo))
	for _, o := range oo {
		addr.SetObject(o)
		res = append(res, addr)
	}

	return res
}
