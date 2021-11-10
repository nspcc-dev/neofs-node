package shard

import (
	"fmt"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// SelectPrm groups the parameters of Select operation.
type SelectPrm struct {
	cid     *cid.ID
	filters objectSDK.SearchFilters
}

// SelectRes groups resulting values of Select operation.
type SelectRes struct {
	addrList []*objectSDK.Address
}

// WithContainerID is a Select option to set the container id to search in.
func (p *SelectPrm) WithContainerID(cid *cid.ID) *SelectPrm {
	if p != nil {
		p.cid = cid
	}

	return p
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
	addrList, err := meta.Select(s.metaBase, prm.cid, prm.filters)
	if err != nil {
		return nil, fmt.Errorf("could not select objects from metabase: %w", err)
	}

	return &SelectRes{
		addrList: addrList,
	}, nil
}
