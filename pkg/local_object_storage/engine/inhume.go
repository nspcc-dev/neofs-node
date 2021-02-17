package engine

import (
	"context"
	"errors"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"go.uber.org/zap"
)

// InhumePrm encapsulates parameters for inhume operation.
type InhumePrm struct {
	addr, tombstone *objectSDK.Address
}

// InhumeRes encapsulates results of inhume operation.
type InhumeRes struct{}

// WithTarget sets object address that should be inhumed and tombstone address
// as the reason for inhume operation.
func (p *InhumePrm) WithTarget(addr, tombstone *objectSDK.Address) *InhumePrm {
	if p != nil {
		p.addr = addr
		p.tombstone = tombstone
	}

	return p
}

var errInhumeFailure = errors.New("inhume operation failed")

// Inhume calls metabase. Inhume method to mark object as removed. It won't be
// removed physically from shard until `Delete` operation.
func (e *StorageEngine) Inhume(prm *InhumePrm) (*InhumeRes, error) {
	shPrm := new(shard.InhumePrm).WithTarget(prm.tombstone, prm.addr)

	res := e.inhume(prm.addr, shPrm, true)
	if res == nil {
		res = e.inhume(prm.addr, shPrm, false)
		if res == nil {
			return nil, errInhumeFailure
		}
	}

	return res, nil
}

func (e *StorageEngine) inhume(addr *objectSDK.Address, prm *shard.InhumePrm, checkExists bool) (res *InhumeRes) {
	e.iterateOverSortedShards(addr, func(_ int, sh *shard.Shard) (stop bool) {
		if checkExists {
			exRes, err := sh.Exists(new(shard.ExistsPrm).
				WithAddress(addr),
			)
			if err != nil {
				// TODO: smth wrong with shard, need to be processed
				e.log.Warn("could not check for presents in shard",
					zap.Stringer("shard", sh.ID()),
					zap.String("error", err.Error()),
				)

				return
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
			res = new(InhumeRes)
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
