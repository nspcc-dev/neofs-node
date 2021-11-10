package engine

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// InhumePrm encapsulates parameters for inhume operation.
type InhumePrm struct {
	tombstone *objectSDK.Address
	addrs     []*objectSDK.Address
}

// InhumeRes encapsulates results of inhume operation.
type InhumeRes struct{}

// WithTarget sets list of objects that should be inhumed and tombstone address
// as the reason for inhume operation.
//
// tombstone should not be nil, addr should not be empty.
// Should not be called along with MarkAsGarbage.
func (p *InhumePrm) WithTarget(tombstone *objectSDK.Address, addrs ...*objectSDK.Address) *InhumePrm {
	if p != nil {
		p.addrs = addrs
		p.tombstone = tombstone
	}

	return p
}

// MarkAsGarbage marks object to be physically removed from local storage.
//
// Should not be called along with WithTarget.
func (p *InhumePrm) MarkAsGarbage(addrs ...*objectSDK.Address) *InhumePrm {
	if p != nil {
		p.addrs = addrs
		p.tombstone = nil
	}

	return p
}

var errInhumeFailure = errors.New("inhume operation failed")

// Inhume calls metabase. Inhume method to mark object as removed. It won't be
// removed physically from shard until `Delete` operation.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Inhume(prm *InhumePrm) (res *InhumeRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.inhume(prm)
		return err
	})

	return
}

func (e *StorageEngine) inhume(prm *InhumePrm) (*InhumeRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddInhumeDuration)()
	}

	shPrm := new(shard.InhumePrm)

	for i := range prm.addrs {
		if prm.tombstone != nil {
			shPrm.WithTarget(prm.tombstone, prm.addrs[i])
		} else {
			shPrm.MarkAsGarbage(prm.addrs[i])
		}

		ok := e.inhumeAddr(prm.addrs[i], shPrm, true)
		if !ok {
			ok = e.inhumeAddr(prm.addrs[i], shPrm, false)
			if !ok {
				return nil, errInhumeFailure
			}
		}
	}

	return new(InhumeRes), nil
}

func (e *StorageEngine) inhumeAddr(addr *objectSDK.Address, prm *shard.InhumePrm, checkExists bool) (ok bool) {
	root := false

	e.iterateOverSortedShards(addr, func(_ int, sh *shard.Shard) (stop bool) {
		defer func() {
			// if object is root we continue since information about it
			// can be presented in other shards
			if checkExists && root {
				stop = false
			}
		}()

		if checkExists {
			exRes, err := sh.Exists(new(shard.ExistsPrm).
				WithAddress(addr),
			)
			if err != nil {
				if errors.Is(err, object.ErrAlreadyRemoved) {
					// inhumed once - no need to be inhumed again
					ok = true
					return true
				}

				var siErr *objectSDK.SplitInfoError
				if !errors.As(err, &siErr) {
					// TODO: smth wrong with shard, need to be processed
					e.log.Warn("could not check for presents in shard",
						zap.Stringer("shard", sh.ID()),
						zap.String("error", err.Error()),
					)

					return
				}

				root = true
			} else if !exRes.Exists() {
				return
			}
		}

		_, err := sh.Inhume(prm)
		if err != nil {
			// TODO: smth wrong with shard, need to be processed
			e.log.Warn("could not inhume object in shard",
				zap.Stringer("shard", sh.ID()),
				zap.String("error", err.Error()),
			)
		} else {
			ok = true
		}

		return err == nil
	})

	return
}

func (e *StorageEngine) processExpiredTombstones(ctx context.Context, addrs []*objectSDK.Address) {
	tss := make(map[string]struct{}, len(addrs))

	for i := range addrs {
		tss[addrs[i].String()] = struct{}{}
	}

	e.iterateOverUnsortedShards(func(sh *shard.Shard) (stop bool) {
		sh.HandleExpiredTombstones(tss)

		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	})
}
