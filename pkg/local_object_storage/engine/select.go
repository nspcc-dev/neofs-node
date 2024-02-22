package engine

import (
	"errors"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// SelectPrm groups the parameters of Select operation.
type SelectPrm struct {
	cnr     cid.ID
	filters object.SearchFilters
}

// SelectRes groups the resulting values of Select operation.
type SelectRes struct {
	addrList []oid.Address
}

// WithContainerID is a Select option to set the container id to search in.
func (p *SelectPrm) WithContainerID(cnr cid.ID) {
	p.cnr = cnr
}

// WithFilters is a Select option to set the object filters.
func (p *SelectPrm) WithFilters(fs object.SearchFilters) {
	p.filters = fs
}

// AddressList returns list of addresses of the selected objects.
func (r SelectRes) AddressList() []oid.Address {
	return r.addrList
}

// Select selects the objects from local storage that match select parameters.
//
// Returns any error encountered that did not allow to completely select the objects.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Select(prm SelectPrm) (res SelectRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e._select(prm)
		return err
	})

	return
}

func (e *StorageEngine) _select(prm SelectPrm) (SelectRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddSearchDuration)()
	}

	addrList := make([]oid.Address, 0)
	uniqueMap := make(map[string]struct{})

	var outError error

	var shPrm shard.SelectPrm
	shPrm.SetContainerID(prm.cnr)
	shPrm.SetFilters(prm.filters)

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

	return SelectRes{
		addrList: addrList,
	}, outError
}

// List returns `limit` available physically storage object addresses in engine.
// If limit is zero, then returns all available object addresses.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) List(limit uint64) (res SelectRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.list(limit)
		return err
	})

	return
}

func (e *StorageEngine) list(limit uint64) (SelectRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddListObjectsDuration)()
	}

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

	return SelectRes{
		addrList: addrList,
	}, nil
}

// Select selects objects from local storage using provided filters.
func Select(storage *StorageEngine, cnr cid.ID, fs object.SearchFilters) ([]oid.Address, error) {
	var selectPrm SelectPrm
	selectPrm.WithContainerID(cnr)
	selectPrm.WithFilters(fs)

	res, err := storage.Select(selectPrm)
	if err != nil {
		return nil, err
	}

	return res.AddressList(), nil
}

// List returns `limit` available physically storage object addresses in
// engine. If limit is zero, then returns all available object addresses.
func List(storage *StorageEngine, limit uint64) ([]oid.Address, error) {
	res, err := storage.List(limit)
	if err != nil {
		return nil, err
	}

	return res.AddressList(), nil
}
