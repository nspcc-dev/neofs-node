package engine

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"go.uber.org/zap"
)

// SelectPrm groups the parameters of Select operation.
type SelectPrm struct {
	filters object.SearchFilters
}

// SelectRes groups resulting values of Select operation.
type SelectRes struct {
	addrList []*object.Address
}

// WithFilters is a Select option to set the object filters.
func (p *SelectPrm) WithFilters(fs objectSDK.SearchFilters) *SelectPrm {
	if p != nil {
		p.filters = fs
	}

	return p
}

// AddressList returns list of addresses of the selected objects.
func (r *SelectRes) AddressList() []*objectSDK.Address {
	return r.addrList
}

// Select selects the objects from local storage that match select parameters.
//
// Returns any error encountered that did not allow to completely select the objects.
func (e *StorageEngine) Select(prm *SelectPrm) (*SelectRes, error) {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	addrList := make([]*object.Address, 0)

	shPrm := new(shard.SelectPrm).
		WithFilters(prm.filters)

	e.iterateOverSortedShards(nil, func(sh *shard.Shard) (stop bool) {
		res, err := sh.Select(shPrm)
		if err != nil {
			// TODO: smth wrong with shard, need to be processed
			e.log.Warn("could not select objects from shard",
				zap.Stringer("shard", sh.ID()),
				zap.String("error", err.Error()),
			)
		} else {
			addrList = append(addrList, res.AddressList()...)
		}

		return false
	})

	return &SelectRes{
		addrList: addrList,
	}, nil
}
