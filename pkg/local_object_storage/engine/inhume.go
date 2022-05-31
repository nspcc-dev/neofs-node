package engine

import (
	"context"
	"errors"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// InhumePrm encapsulates parameters for inhume operation.
type InhumePrm struct {
	tombstone *oid.Address
	addrs     []oid.Address
}

// InhumeRes encapsulates results of inhume operation.
type InhumeRes struct{}

// WithTarget sets a list of objects that should be inhumed and tombstone address
// as the reason for inhume operation.
//
// tombstone should not be nil, addr should not be empty.
// Should not be called along with MarkAsGarbage.
func (p *InhumePrm) WithTarget(tombstone oid.Address, addrs ...oid.Address) {
	if p != nil {
		p.addrs = addrs
		p.tombstone = &tombstone
	}
}

// MarkAsGarbage marks an object to be physically removed from local storage.
//
// Should not be called along with WithTarget.
func (p *InhumePrm) MarkAsGarbage(addrs ...oid.Address) {
	if p != nil {
		p.addrs = addrs
		p.tombstone = nil
	}
}

var errInhumeFailure = errors.New("inhume operation failed")

// Inhume calls metabase. Inhume method to mark an object as removed. It won't be
// removed physically from the shard until `Delete` operation.
//
// Allows inhuming non-locked objects only. Returns apistatus.ObjectLocked
// if at least one object is locked.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Inhume(prm InhumePrm) (res InhumeRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.inhume(prm)
		return err
	})

	return
}

func (e *StorageEngine) inhume(prm InhumePrm) (InhumeRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddInhumeDuration)()
	}

	var shPrm shard.InhumePrm

	for i := range prm.addrs {
		if prm.tombstone != nil {
			shPrm.WithTarget(*prm.tombstone, prm.addrs[i])
		} else {
			shPrm.MarkAsGarbage(prm.addrs[i])
		}

		switch e.inhumeAddr(prm.addrs[i], shPrm, true) {
		case 1:
			return InhumeRes{}, apistatus.ObjectLocked{}
		case 0:
			switch e.inhumeAddr(prm.addrs[i], shPrm, false) {
			case 1:
				return InhumeRes{}, apistatus.ObjectLocked{}
			case 0:
				return InhumeRes{}, errInhumeFailure
			}
		}
	}

	return InhumeRes{}, nil
}

// Returns:
//   0 - fail
//   1 - object locked
//   2 - ok
func (e *StorageEngine) inhumeAddr(addr oid.Address, prm shard.InhumePrm, checkExists bool) (status uint8) {
	root := false
	var errLocked apistatus.ObjectLocked
	var existPrm shard.ExistsPrm

	e.iterateOverSortedShards(addr, func(_ int, sh hashedShard) (stop bool) {
		defer func() {
			// if object is root we continue since information about it
			// can be presented in other shards
			if checkExists && root {
				stop = false
			}
		}()

		if checkExists {
			existPrm.WithAddress(addr)
			exRes, err := sh.Exists(existPrm)
			if err != nil {
				if shard.IsErrRemoved(err) {
					// inhumed once - no need to be inhumed again
					status = 2
					return true
				}

				var siErr *objectSDK.SplitInfoError
				if !errors.As(err, &siErr) {
					e.reportShardError(sh, "could not check for presents in shard", err)
					return
				}

				root = true
			} else if !exRes.Exists() {
				return
			}
		}

		_, err := sh.Inhume(prm)
		if err != nil {
			e.reportShardError(sh, "could not inhume object in shard", err)

			if errors.As(err, &errLocked) {
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

func (e *StorageEngine) processExpiredTombstones(ctx context.Context, addrs []meta.TombstonedObject) {
	e.iterateOverUnsortedShards(func(sh hashedShard) (stop bool) {
		sh.HandleExpiredTombstones(addrs)

		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	})
}

func (e *StorageEngine) processExpiredLocks(ctx context.Context, lockers []oid.Address) {
	e.iterateOverUnsortedShards(func(sh hashedShard) (stop bool) {
		sh.HandleExpiredLocks(lockers)

		select {
		case <-ctx.Done():
			e.log.Info("interrupt processing the expired locks by context")
			return true
		default:
			return false
		}
	})
}
