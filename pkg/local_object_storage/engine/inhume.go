package engine

import (
	"errors"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
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
				zap.Stringer("cid", cID),
				zap.Stringer("shard", sh.ID()),
				zap.Error(err))
		}
	}

	return nil
}

// processAddrDelete processes deletion (inhume or immediate delete) of an object by its address.
func (e *StorageEngine) processAddrDelete(addr oid.Address, deleteFunc func(*shard.Shard, []oid.Address) error) error {
	var (
		children []oid.Address
		err      error
		root     bool
		siNoLink *object.SplitInfo
		shards   = e.sortedShards(addr)
	)

	// see if the object is root
	for _, sh := range shards {
		exists, err := sh.Exists(addr, true)
		if err != nil {
			if shard.IsErrNotFound(err) {
				continue
			}

			if shard.IsErrRemoved(err) {
				// inhumed once - no need to be inhumed again
				return nil
			}

			var errECParts iec.ErrParts
			if errors.As(err, &errECParts) {
				// TODO: Boolean switch, we don't need errECParts elements. Support and do fast return from Exists().
				root = true
				break
			}

			var siErr *object.SplitInfoError
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
				siNoLink = siErr.SplitInfo()
				// keep searching for the link object
				continue
			}

			siNoLink = nil

			var linkAddr oid.Address
			linkAddr.SetContainer(addr.Container())
			linkAddr.SetObject(linkID)

			linkObj, err := e.Get(linkAddr)
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

				children = measuredObjsToAddresses(addr.Container(), link.Objects())
			} else {
				// v1 split
				children = oIDsToAddresses(addr.Container(), linkObj.Children())
			}

			children = append(children, linkAddr)

			break
		}

		if !exists {
			continue
		}

		err = deleteFunc(sh.Shard, []oid.Address{addr})
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
		addrs  = append(children, addr)
		ok     bool
		retErr error
	)

	// has not found the object on any shard, so delete on the most probable one
	for _, sh := range shards {
		err = deleteFunc(sh.Shard, addrs)
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

func (e *StorageEngine) collectChildrenWithoutLink(addr oid.Address, si *object.SplitInfo) []oid.Address {
	e.log.Info("root object has no link object in split upload",
		zap.Stringer("addrBeingInhumed", addr))

	firstID := si.GetFirstPart()
	splitID := si.SplitID()
	switch {
	case !firstID.IsZero():
		res, err := e.collectRawWithAttribute(addr.Container(), object.FilterFirstSplitObject, firstID[:])
		if err == nil {
			res = append(res, oid.NewAddress(addr.Container(), firstID))
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
func (e *StorageEngine) IsLocked(addr oid.Address) (bool, error) {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return false, e.blockErr
	}

	return e.isLocked(addr)
}

func (e *StorageEngine) isLocked(addr oid.Address) (bool, error) {
	for _, sh := range e.unsortedShards() {
		locked, err := sh.IsLocked(addr)
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
	var rLocked = e.blockMtx.TryRLock()

	if !rLocked || e.blockErr != nil {
		return
	}
	defer e.blockMtx.RUnlock()

	for _, addr := range addrs {
		locked, err := e.isLocked(addr)
		if err != nil {
			e.log.Warn("removing an object without full locking check",
				zap.Error(err),
				zap.Stringer("addr", addr))
		} else if locked {
			e.log.Warn("skip an expired object with lock",
				zap.Stringer("addr", addr))
			continue
		}

		err = e.processAddrDelete(addr, (*shard.Shard).Delete)
		if err != nil {
			e.log.Warn("deleting expired object", zap.Stringer("addr", addr), zap.Error(err))
		}
	}
}

func measuredObjsToAddresses(cID cid.ID, mm []object.MeasuredObject) []oid.Address {
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
