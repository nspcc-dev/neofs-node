package shard

import (
	"fmt"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// Cursor is a type for continuous object listing.
type Cursor = meta.Cursor

// ErrEndOfListing is returned from object listing with cursor
// when storage can't return any more objects after provided
// cursor. Use nil cursor object to start listing again.
var ErrEndOfListing = meta.ErrEndOfListing

type ListContainersPrm struct{}

type ListContainersRes struct {
	containers []cid.ID
}

func (r ListContainersRes) Containers() []cid.ID {
	return r.containers
}

// ListWithCursorPrm contains parameters for ListWithCursor operation.
type ListWithCursorPrm struct {
	count  uint32
	cursor *Cursor
}

// ListWithCursorRes contains values returned from ListWithCursor operation.
type ListWithCursorRes struct {
	addrList []objectcore.AddressWithType
	cursor   *Cursor
}

// WithCount sets maximum amount of addresses that ListWithCursor should return.
func (p *ListWithCursorPrm) WithCount(count uint32) {
	p.count = count
}

// WithCursor sets cursor for ListWithCursor operation. For initial request,
// ignore this param or use nil value. For consecutive requests, use value
// from ListWithCursorRes.
func (p *ListWithCursorPrm) WithCursor(cursor *Cursor) {
	p.cursor = cursor
}

// AddressList returns addresses selected by ListWithCursor operation.
func (r ListWithCursorRes) AddressList() []objectcore.AddressWithType {
	return r.addrList
}

// Cursor returns cursor for consecutive listing requests.
func (r ListWithCursorRes) Cursor() *Cursor {
	return r.cursor
}

// List returns all objects physically stored in the Shard.
func (s *Shard) List() (res SelectRes, err error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.NoMetabase() {
		return SelectRes{}, ErrDegradedMode
	}

	lst, err := s.metaBase.Containers()
	if err != nil {
		return res, fmt.Errorf("can't list stored containers: %w", err)
	}

	filters := object.NewSearchFilters()
	filters.AddPhyFilter()

	for i := range lst {
		var sPrm meta.SelectPrm
		sPrm.SetContainerID(lst[i])
		sPrm.SetFilters(filters)

		sRes, err := s.metaBase.Select(sPrm) // consider making List in metabase
		if err != nil {
			s.log.Debug("can't select all objects",
				zap.Stringer("cid", lst[i]),
				zap.String("error", err.Error()))

			continue
		}

		res.addrList = append(res.addrList, sRes.AddressList()...)
	}

	return res, nil
}

func (s *Shard) ListContainers(_ ListContainersPrm) (ListContainersRes, error) {
	if s.GetMode().NoMetabase() {
		return ListContainersRes{}, ErrDegradedMode
	}

	containers, err := s.metaBase.Containers()
	if err != nil {
		return ListContainersRes{}, fmt.Errorf("could not get list of containers: %w", err)
	}

	return ListContainersRes{
		containers: containers,
	}, nil
}

// ListWithCursor lists physical objects available in shard starting from
// cursor. Includes regular, tombstone and storage group objects. Does not
// include inhumed objects. Use cursor value from response for consecutive requests.
//
// Returns ErrEndOfListing if there are no more objects to return or count
// parameter set to zero.
func (s *Shard) ListWithCursor(prm ListWithCursorPrm) (ListWithCursorRes, error) {
	if s.GetMode().NoMetabase() {
		return ListWithCursorRes{}, ErrDegradedMode
	}

	var metaPrm meta.ListPrm
	metaPrm.SetCount(prm.count)
	metaPrm.SetCursor(prm.cursor)
	res, err := s.metaBase.ListWithCursor(metaPrm)
	if err != nil {
		return ListWithCursorRes{}, fmt.Errorf("could not get list of objects: %w", err)
	}

	return ListWithCursorRes{
		addrList: res.AddressList(),
		cursor:   res.Cursor(),
	}, nil
}
