package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addr oid.Address

	forceRemoval bool
}

// DeleteRes groups the resulting values of Delete operation.
type DeleteRes struct{}

// WithAddress is a Delete option to set the addresses of the objects to delete.
//
// Option is required.
func (p *DeletePrm) WithAddress(addr oid.Address) {
	p.addr = addr
}

// WithForceRemoval is a Delete option to remove an object despite any
// restrictions imposed on deleting that object. Expected to be used
// only in control service.
func (p *DeletePrm) WithForceRemoval() {
	p.forceRemoval = true
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
	var splitInfo *objectSDK.SplitInfo

	// Removal of a big object is done in multiple stages:
	// 1. Remove the parent object. If it is locked or already removed, return immediately.
	// 2. Otherwise, search for all objects with a particular SplitID and delete them too.
	e.iterateOverSortedShards(prm.addr, func(_ int, sh hashedShard) (stop bool) {
		var existsPrm shard.ExistsPrm
		existsPrm.SetAddress(prm.addr)

		resExists, err := sh.Exists(existsPrm)
		if err != nil {
			if shard.IsErrRemoved(err) || shard.IsErrObjectExpired(err) {
				return true
			}

			var splitErr *objectSDK.SplitInfoError
			if !errors.As(err, &splitErr) {
				if !shard.IsErrNotFound(err) {
					e.reportShardError(sh, "could not check object existence", err)
				}
				return false
			}
			splitInfo = splitErr.SplitInfo()
		} else if !resExists.Exists() {
			return false
		}

		var shPrm shard.InhumePrm
		shPrm.MarkAsGarbage(prm.addr)
		if prm.forceRemoval {
			shPrm.ForceRemoval()
		}

		_, err = sh.Inhume(shPrm)
		if err != nil {
			e.reportShardError(sh, "could not inhume object in shard", err)

			locked.is = errors.As(err, &locked.err)

			return locked.is
		}

		// If a parent object is removed we should set GC mark on each shard.
		return splitInfo == nil
	})

	if locked.is {
		return DeleteRes{}, locked.err
	}

	if splitInfo != nil {
		if splitID := splitInfo.SplitID(); splitID != nil {
			e.deleteChildren(prm.addr, prm.forceRemoval, *splitID)
		}
	}

	return DeleteRes{}, nil
}

func (e *StorageEngine) deleteChildren(addr oid.Address, force bool, splitID objectSDK.SplitID) {
	var fs objectSDK.SearchFilters
	fs.AddSplitIDFilter(objectSDK.MatchStringEqual, splitID)

	var selectPrm shard.SelectPrm
	selectPrm.SetFilters(fs)
	selectPrm.SetContainerID(addr.Container())

	var inhumePrm shard.InhumePrm
	if force {
		inhumePrm.ForceRemoval()
	}

	e.iterateOverSortedShards(addr, func(_ int, sh hashedShard) (stop bool) {
		res, err := sh.Select(selectPrm)
		if err != nil {
			e.log.Warn("error during searching for object children",
				zap.Stringer("addr", addr),
				zap.String("error", err.Error()))
			return false
		}

		for _, addr := range res.AddressList() {
			inhumePrm.MarkAsGarbage(addr)

			_, err = sh.Inhume(inhumePrm)
			if err != nil {
				e.log.Debug("could not inhume object in shard",
					zap.Stringer("addr", addr),
					zap.String("err", err.Error()))
				continue
			}
		}
		return false
	})
}
