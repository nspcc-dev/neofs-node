package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/object/address"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

var errLockFailed = errors.New("lock operation failed")

// Lock marks objects as locked with another object. All objects from the
// specified container.
//
// Allows locking regular objects only (otherwise returns apistatus.IrregularObjectLock).
//
// Locked list should be unique. Panics if it is empty.
func (e *StorageEngine) Lock(idCnr cid.ID, locker oid.ID, locked []oid.ID) error {
	return e.execIfNotBlocked(func() error {
		return e.lock(idCnr, locker, locked)
	})
}

func (e *StorageEngine) lock(idCnr cid.ID, locker oid.ID, locked []oid.ID) error {
	var addr address.Address
	addr.SetContainerID(&idCnr)

	for i := range locked {
		switch e.lockSingle(idCnr, locker, locked[i], true) {
		case 1:
			return apistatus.LockNonRegularObject{}
		case 0:
			switch e.lockSingle(idCnr, locker, locked[i], false) {
			case 1:
				return apistatus.LockNonRegularObject{}
			case 0:
				return errLockFailed
			}
		}
	}

	return nil
}

// Returns:
//   0 - fail
//   1 - locking irregular object
//   2 - ok
func (e *StorageEngine) lockSingle(idCnr cid.ID, locker, locked oid.ID, checkExists bool) (status uint8) {
	// code is pretty similar to inhumeAddr, maybe unify?
	root := false
	var errIrregular apistatus.LockNonRegularObject

	var addrLocked address.Address
	addrLocked.SetContainerID(&idCnr)
	addrLocked.SetObjectID(&locked)

	e.iterateOverSortedShards(&addrLocked, func(_ int, sh hashedShard) (stop bool) {
		defer func() {
			// if object is root we continue since information about it
			// can be presented in other shards
			if checkExists && root {
				stop = false
			}
		}()

		if checkExists {
			exRes, err := sh.Exists(new(shard.ExistsPrm).
				WithAddress(&addrLocked),
			)
			if err != nil {
				var siErr *objectSDK.SplitInfoError
				if !errors.As(err, &siErr) {
					e.reportShardError(sh, "could not check locked object for presence in shard", err)
					return
				}

				root = true
			} else if !exRes.Exists() {
				return
			}
		}

		err := sh.Lock(idCnr, locker, []oid.ID{locked})
		if err != nil {
			e.reportShardError(sh, "could not lock object in shard", err)

			if errors.As(err, &errIrregular) {
				status = 1
				return true
			}

			return false
		}

		status = 2

		return true
	})

	return
}
