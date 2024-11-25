package shard

import (
	"fmt"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Cursor is a type for continuous object listing.
type Cursor = meta.Cursor

// ErrEndOfListing is returned from object listing with cursor
// when storage can't return any more objects after provided
// cursor. Use nil cursor object to start listing again.
var ErrEndOfListing = meta.ErrEndOfListing

// List returns all objects physically stored in the Shard.
func (s *Shard) List() ([]oid.Address, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	lst, err := s.metaBase.Containers()
	if err != nil {
		return nil, fmt.Errorf("can't list stored containers: %w", err)
	}

	filters := object.NewSearchFilters()
	filters.AddPhyFilter()

	var res []oid.Address

	for i := range lst {
		addrs, err := s.metaBase.Select(lst[i], filters) // consider making List in metabase
		if err != nil {
			s.log.Debug("can't select all objects",
				zap.Stringer("cid", lst[i]),
				zap.Error(err))

			continue
		}

		res = append(res, addrs...)
	}

	return res, nil
}

// ListContainers enumerates all containers known to this shard.
func (s *Shard) ListContainers() ([]cid.ID, error) {
	if s.GetMode().NoMetabase() {
		return nil, ErrDegradedMode
	}

	containers, err := s.metaBase.Containers()
	if err != nil {
		return nil, fmt.Errorf("could not get list of containers: %w", err)
	}

	return containers, nil
}

// ListWithCursor lists physical objects available in shard starting from
// cursor. Includes regular, tombstone and storage group objects. Does not
// include inhumed objects. Use cursor value from response for consecutive requests.
//
// Returns ErrEndOfListing if there are no more objects to return or count
// parameter set to zero.
func (s *Shard) ListWithCursor(count int, cursor *Cursor) ([]objectcore.AddressWithType, *Cursor, error) {
	if s.GetMode().NoMetabase() {
		return nil, nil, ErrDegradedMode
	}

	addrs, cursor, err := s.metaBase.ListWithCursor(count, cursor)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get list of objects: %w", err)
	}

	return addrs, cursor, nil
}
