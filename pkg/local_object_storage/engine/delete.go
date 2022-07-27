package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addr []oid.Address

	forceRemoval bool
}

// DeleteRes groups the resulting values of Delete operation.
type DeleteRes struct{}

// WithAddresses is a Delete option to set the addresses of the objects to delete.
//
// Option is required.
func (p *DeletePrm) WithAddresses(addr ...oid.Address) {
	if p != nil {
		p.addr = append(p.addr, addr...)
	}
}

// WithForceRemoval is a Delete option to remove an object despite any
// restrictions imposed on deleting that object. Expected to be used
// only in control service.
func (p *DeletePrm) WithForceRemoval() {
	if p != nil {
		p.forceRemoval = true
	}
}

// Delete marks the objects to be removed.
//
// Returns an error if executions are blocked (see BlockExecution).
//
// Returns apistatus.ObjectLocked if at least one object is locked.
// In this case no object from the list is marked to be deleted.
//
// NOTE: Marks any object to be deleted (despite any prohibitions
// on operations with that object) if WithForceRemoval option has
// been provided.
func (e *StorageEngine) Delete(prm DeletePrm) (res DeleteRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.delete(prm)
		return err
	})

	return
}

func (e *StorageEngine) delete(prm DeletePrm) (DeleteRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddDeleteDuration)()
	}

	var locked struct {
		is  bool
		err apistatus.ObjectLocked
	}

	for i := range prm.addr {
		e.iterateOverSortedShards(prm.addr[i], func(_ int, sh hashedShard) (stop bool) {
			var existsPrm shard.ExistsPrm
			existsPrm.SetAddress(prm.addr[i])

			resExists, err := sh.Exists(existsPrm)
			if err != nil {
				_, ok := err.(*objectSDK.SplitInfoError)
				if ok || shard.IsErrRemoved(err) || shard.IsErrObjectExpired(err) {
					return true
				}
				if !shard.IsErrNotFound(err) {
					e.reportShardError(sh, "could not check object existence", err)
				}
				return false
			} else if !resExists.Exists() {
				return false
			}

			var shPrm shard.InhumePrm
			shPrm.MarkAsGarbage(prm.addr[i])
			if prm.forceRemoval {
				shPrm.ForceRemoval()
			}

			_, err = sh.Inhume(shPrm)
			if err != nil {
				e.reportShardError(sh, "could not inhume object in shard", err)

				locked.is = errors.As(err, &locked.err)

				return locked.is
			}

			return true
		})
	}

	if locked.is {
		return DeleteRes{}, locked.err
	}

	return DeleteRes{}, nil
}
