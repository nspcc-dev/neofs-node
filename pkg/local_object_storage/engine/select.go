package engine

import (
	"errors"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Select selects the objects from local storage that match select parameters.
//
// Returns any error encountered that did not allow to completely select the objects.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Select(cnr cid.ID, filters object.SearchFilters) ([]oid.Address, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddSearchDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	addrList := make([]oid.Address, 0)
	uniqueMap := make(map[string]struct{})

	for _, sh := range e.unsortedShards() {
		res, err := sh.Select(cnr, filters)
		if err != nil {
			if errors.Is(err, objectcore.ErrInvalidSearchQuery) {
				return addrList, err
			}
			e.reportShardError(sh, "could not select objects from shard", err)
			continue
		}

		for _, addr := range res { // save only unique values
			if _, ok := uniqueMap[addr.EncodeToString()]; !ok {
				uniqueMap[addr.EncodeToString()] = struct{}{}
				addrList = append(addrList, addr)
			}
		}
	}

	return addrList, nil
}

// List returns `limit` available physically storage object addresses in engine.
// If limit is zero, then returns all available object addresses.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) List(limit uint64) ([]oid.Address, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddListObjectsDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	addrList := make([]oid.Address, 0, limit)
	uniqueMap := make(map[string]struct{})
	ln := uint64(0)

	// consider iterating over shuffled shards
	for _, sh := range e.unsortedShards() {
		res, err := sh.List() // consider limit result of shard iterator
		if err != nil {
			e.reportShardError(sh, "could not select objects from shard", err)
			continue
		}
		for _, addr := range res { // save only unique values
			if _, ok := uniqueMap[addr.EncodeToString()]; !ok {
				uniqueMap[addr.EncodeToString()] = struct{}{}
				addrList = append(addrList, addr)

				ln++
				if limit > 0 && ln >= limit {
					return addrList, nil
				}
			}
		}
	}

	return addrList, nil
}
