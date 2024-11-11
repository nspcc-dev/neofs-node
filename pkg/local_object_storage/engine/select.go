package engine

import (
	"errors"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
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
	var (
		err error
		res []oid.Address
	)

	if e.metrics != nil {
		defer elapsed(e.metrics.AddSearchDuration)()
	}

	err = e.execIfNotBlocked(func() error {
		res, err = e._select(cnr, filters)
		return err
	})

	return res, err
}

func (e *StorageEngine) _select(cnr cid.ID, filters object.SearchFilters) ([]oid.Address, error) {
	addrList := make([]oid.Address, 0)
	uniqueMap := make(map[string]struct{})

	var outError error

	var shPrm shard.SelectPrm
	shPrm.SetContainerID(cnr)
	shPrm.SetFilters(filters)

	e.iterateOverUnsortedShards(func(sh hashedShard) (stop bool) {
		res, err := sh.Select(shPrm)
		if err != nil {
			if errors.Is(err, objectcore.ErrInvalidSearchQuery) {
				outError = err
				return true
			}
			e.reportShardError(sh, "could not select objects from shard", err)
			return false
		}

		for _, addr := range res.AddressList() { // save only unique values
			if _, ok := uniqueMap[addr.EncodeToString()]; !ok {
				uniqueMap[addr.EncodeToString()] = struct{}{}
				addrList = append(addrList, addr)
			}
		}

		return false
	})

	return addrList, outError
}

// List returns `limit` available physically storage object addresses in engine.
// If limit is zero, then returns all available object addresses.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) List(limit uint64) ([]oid.Address, error) {
	var (
		err error
		res []oid.Address
	)

	if e.metrics != nil {
		defer elapsed(e.metrics.AddListObjectsDuration)()
	}

	err = e.execIfNotBlocked(func() error {
		res, err = e.list(limit)
		return err
	})

	return res, err
}

func (e *StorageEngine) list(limit uint64) ([]oid.Address, error) {
	addrList := make([]oid.Address, 0, limit)
	uniqueMap := make(map[string]struct{})
	ln := uint64(0)

	// consider iterating over shuffled shards
	e.iterateOverUnsortedShards(func(sh hashedShard) (stop bool) {
		res, err := sh.List() // consider limit result of shard iterator
		if err != nil {
			e.reportShardError(sh, "could not select objects from shard", err)
		} else {
			for _, addr := range res.AddressList() { // save only unique values
				if _, ok := uniqueMap[addr.EncodeToString()]; !ok {
					uniqueMap[addr.EncodeToString()] = struct{}{}
					addrList = append(addrList, addr)

					ln++
					if limit > 0 && ln >= limit {
						return true
					}
				}
			}
		}

		return false
	})

	return addrList, nil
}

// Select selects objects from local storage using provided filters.
func Select(storage *StorageEngine, cnr cid.ID, fs object.SearchFilters) ([]oid.Address, error) {
	return storage.Select(cnr, fs)
}

// List returns `limit` available physically storage object addresses in
// engine. If limit is zero, then returns all available object addresses.
func List(storage *StorageEngine, limit uint64) ([]oid.Address, error) {
	return storage.List(limit)
}
