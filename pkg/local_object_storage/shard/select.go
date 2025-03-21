package shard

import (
	"fmt"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Select selects the objects from shard that match select parameters.
//
// Returns any error encountered that
// did not allow to completely select the objects.
//
// Returns [object.ErrInvalidSearchQuery] if specified query is invalid.
func (s *Shard) Select(cnr cid.ID, filters object.SearchFilters) ([]oid.Address, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	addrs, err := s.metaBase.Select(cnr, filters)
	if err != nil {
		return nil, fmt.Errorf("could not select objects from metabase: %w", err)
	}

	return addrs, nil
}

// Search performs Search op on the underlying metabase if it is not disabled.
func (s *Shard) Search(cnr cid.ID, fs object.SearchFilters, fInt map[int]objectcore.ParsedIntFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	if s.info.Mode.NoMetabase() {
		return nil, nil, ErrDegradedMode
	}
	res, newCursor, err := s.metaBase.Search(cnr, fs, fInt, attrs, cursor, count)
	if err != nil {
		return nil, nil, fmt.Errorf("call metabase: %w", err)
	}
	return res, newCursor, nil
}
