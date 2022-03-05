package engine

import (
	"errors"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// SelectPrm groups the parameters of Select operation.
type SelectPrm struct {
	cid     *cid.ID
	filters object.SearchFilters
}

// SelectRes groups resulting values of Select operation.
type SelectRes struct {
	addrList []*object.Address
}

// WithContainerID is a Select option to set the container id to search in.
func (p *SelectPrm) WithContainerID(cid *cid.ID) *SelectPrm {
	if p != nil {
		p.cid = cid
	}

	return p
}

// WithFilters is a Select option to set the object filters.
func (p *SelectPrm) WithFilters(fs object.SearchFilters) *SelectPrm {
	if p != nil {
		p.filters = fs
	}

	return p
}

// AddressList returns list of addresses of the selected objects.
func (r *SelectRes) AddressList() []*object.Address {
	return r.addrList
}

// Select selects the objects from local storage that match select parameters.
//
// Returns any error encountered that did not allow to completely select the objects.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Select(prm *SelectPrm) (res *SelectRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e._select(prm)
		return err
	})

	return
}

func (e *StorageEngine) _select(prm *SelectPrm) (*SelectRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddSearchDuration)()
	}

	addrList := make([]*object.Address, 0)
	uniqueMap := make(map[string]struct{})

	var outError error

	shPrm := new(shard.SelectPrm).
		WithContainerID(prm.cid).
		WithFilters(prm.filters)

	e.iterateOverUnsortedShards(func(sh *shard.Shard) (stop bool) {
		res, err := sh.Select(shPrm)
		if err != nil {
			switch {
			case errors.Is(err, meta.ErrMissingContainerID): // should never happen
				e.log.Error("missing container ID parameter")
				outError = err

				return true
			default:
				// TODO: smth wrong with shard, need to be processed
				e.log.Warn("could not select objects from shard",
					zap.Stringer("shard", sh.ID()),
					zap.String("error", err.Error()),
				)
				return false
			}
		} else {
			for _, addr := range res.AddressList() { // save only unique values
				if _, ok := uniqueMap[addr.String()]; !ok {
					uniqueMap[addr.String()] = struct{}{}
					addrList = append(addrList, addr)
				}
			}
		}

		return false
	})

	return &SelectRes{
		addrList: addrList,
	}, outError
}

// List returns `limit` available physically storage object addresses in engine.
// If limit is zero, then returns all available object addresses.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) List(limit uint64) (res *SelectRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.list(limit)
		return err
	})

	return
}

func (e *StorageEngine) list(limit uint64) (*SelectRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddListObjectsDuration)()
	}

	addrList := make([]*object.Address, 0, limit)
	uniqueMap := make(map[string]struct{})
	ln := uint64(0)

	// consider iterating over shuffled shards
	e.iterateOverUnsortedShards(func(sh *shard.Shard) (stop bool) {
		res, err := sh.List() // consider limit result of shard iterator
		if err != nil {
			// TODO: smth wrong with shard, need to be processed
			e.log.Warn("could not select objects from shard",
				zap.Stringer("shard", sh.ID()),
				zap.String("error", err.Error()),
			)
		} else {
			for _, addr := range res.AddressList() { // save only unique values
				if _, ok := uniqueMap[addr.String()]; !ok {
					uniqueMap[addr.String()] = struct{}{}
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

	return &SelectRes{
		addrList: addrList,
	}, nil
}

// Select selects objects from local storage using provided filters.
func Select(storage *StorageEngine, cid *cid.ID, fs object.SearchFilters) ([]*object.Address, error) {
	res, err := storage.Select(new(SelectPrm).
		WithContainerID(cid).
		WithFilters(fs),
	)
	if err != nil {
		return nil, err
	}

	return res.AddressList(), nil
}

// List returns `limit` available physically storage object addresses in
// engine. If limit is zero, then returns all available object addresses.
func List(storage *StorageEngine, limit uint64) ([]*object.Address, error) {
	res, err := storage.List(limit)
	if err != nil {
		return nil, err
	}

	return res.AddressList(), nil
}
