package tombstone

import (
	"context"
	"strconv"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Source is a tombstone source interface.
type Source interface {
	// Tombstone must return tombstone from the source it was
	// configured to fetch from and any error that appeared during
	// fetching process.
	//
	// Tombstone MUST return (nil, nil) if requested tombstone is
	// missing in the storage for the provided epoch.
	Tombstone(ctx context.Context, a oid.Address, epoch uint64) (*object.Object, error)
}

// ExpirationChecker is a tombstone source wrapper.
// It checks tombstones presence via tombstone
// source, caches it checks its expiration.
//
// Must be created via NewChecker function. `var` and
// `ExpirationChecker{}` declarations leads to undefined behaviour
// and may lead to panics.
type ExpirationChecker struct {
	cache *lru.Cache[oid.Address, uint64]

	log *zap.Logger

	tsSource Source
}

// IsTombstoneAvailable checks the tombstone presence in the system in the
// following order:
//   - 1. Local LRU cache;
//   - 2. Tombstone source.
//
// If a tombstone was successfully fetched (regardless of its expiration)
// it is cached in the LRU cache.
func (g *ExpirationChecker) IsTombstoneAvailable(ctx context.Context, a oid.Address, epoch uint64) bool {
	addrStr := a.EncodeToString()
	log := g.log.With(zap.String("address", addrStr))

	expEpoch, ok := g.cache.Get(a)
	if ok {
		return expEpoch > epoch
	}

	ts, err := g.tsSource.Tombstone(ctx, a, epoch)
	if err != nil {
		log.Warn(
			"tombstone getter: could not get the tombstone the source",
			zap.Error(err),
		)
	} else {
		if ts != nil {
			return g.handleTS(a, ts, epoch)
		}
	}

	// requested tombstone not
	// found in the NeoFS network
	return false
}

func (g *ExpirationChecker) handleTS(addr oid.Address, ts *object.Object, reqEpoch uint64) bool {
	for _, atr := range ts.Attributes() {
		if atr.Key() == object.AttributeExpirationEpoch {
			epoch, err := strconv.ParseUint(atr.Value(), 10, 64)
			if err != nil {
				g.log.Warn(
					"tombstone getter: could not parse tombstone expiration epoch",
					zap.Error(err),
				)

				return false
			}

			g.cache.Add(addr, epoch)
			return epoch >= reqEpoch
		}
	}

	// unexpected tombstone without expiration epoch
	return false
}
