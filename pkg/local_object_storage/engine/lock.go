package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

var errLockFailed = errors.New("lock operation failed")

// Lock marks objects as locked with another object. All objects from the
// specified container.
//
// Allows locking regular objects only (otherwise returns apistatus.LockNonRegularObject).
//
// Locked list should be unique. Panics if it is empty.
func (e *StorageEngine) Lock(idCnr cid.ID, locker oid.ID, locked []oid.ID) error {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return e.blockErr
	}

	for i := range locked {
		switch e.lockSingle(idCnr, locker, locked[i], true) {
		case 1:
			return logicerr.Wrap(apistatus.LockNonRegularObject{})
		case 0:
			switch e.lockSingle(idCnr, locker, locked[i], false) {
			case 1:
				return logicerr.Wrap(apistatus.LockNonRegularObject{})
			case 0:
				return logicerr.Wrap(errLockFailed)
			}
		}
	}

	return nil
}

// Returns:
//   - 0: fail
//   - 1: locking irregular object
//   - 2: ok
func (e *StorageEngine) lockSingle(idCnr cid.ID, locker, locked oid.ID, checkExists bool) uint8 {
	// code is pretty similar to inhumeAddr, maybe unify?
	var (
		addrLocked oid.Address
		root       bool
		status     uint8
	)
	addrLocked.SetContainer(idCnr)
	addrLocked.SetObject(locked)

	for _, sh := range e.sortedShards(addrLocked) {
		if checkExists {
			exists, err := sh.Exists(addrLocked, false)
			if err != nil {
				var siErr *objectSDK.SplitInfoError
				if !errors.As(err, &siErr) {
					if shard.IsErrObjectExpired(err) {
						// object is already expired =>
						// do not lock it
						return 0
					}

					e.reportShardError(sh, "could not check locked object for presence in shard", err)
					if !root {
						return 0
					}
					continue
				}

				root = true
			} else if !exists {
				if !root {
					return 0
				}
				continue
			}
		}

		err := sh.Lock(idCnr, locker, []oid.ID{locked})
		if err != nil {
			var errIrregular apistatus.LockNonRegularObject

			e.reportShardError(sh, "could not lock object in shard", err)

			if errors.As(err, &errIrregular) {
				status = 1
			} else {
				continue
			}
		} else {
			status = 2
		}

		// if object is root we continue since information about it
		// can be presented in other shards
		if !root {
			break
		}
	}

	return status
}
