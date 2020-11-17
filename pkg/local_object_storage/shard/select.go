package shard

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/pkg/errors"
)

// SelectPrm groups the parameters of Select operation.
type SelectPrm struct {
	filters objectSDK.SearchFilters
}

// SelectRes groups resulting values of Select operation.
type SelectRes struct {
	addrList []*objectSDK.Address
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

// Select selects the objects from shard that match select parameters.
//
// Returns any error encountered that
// did not allow to completely select the objects.
func (s *Shard) Select(prm *SelectPrm) (*SelectRes, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	addrList, err := s.metaBase.Select(prm.filters)
	if err != nil {
		return nil, errors.Wrap(err, "could not select objects from metabase")
	}

	return &SelectRes{
		addrList: addrList,
	}, nil
}
