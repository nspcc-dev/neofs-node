package shard

import (
	"fmt"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
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
	if p != nil {
		p.cnr = cnr
	}
}

// WithFilters is a Select option to set the object filters.
func (p *SelectPrm) WithFilters(fs object.SearchFilters) {
	if p != nil {
		p.filters = fs
	}
}

// AddressList returns list of addresses of the selected objects.
func (r SelectRes) AddressList() []oid.Address {
	return r.addrList
}

// Select selects the objects from shard that match select parameters.
//
// Returns any error encountered that
// did not allow to completely select the objects.
func (s *Shard) Select(prm SelectPrm) (SelectRes, error) {
	var selectPrm meta.SelectPrm
	selectPrm.WithFilters(prm.filters)
	selectPrm.WithContainerID(prm.cnr)

	mRes, err := s.metaBase.Select(selectPrm)
	if err != nil {
		return SelectRes{}, fmt.Errorf("could not select objects from metabase: %w", err)
	}

	return SelectRes{
		addrList: mRes.AddressList(),
	}, nil
}
