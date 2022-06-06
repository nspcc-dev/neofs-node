package shard

import (
	"fmt"

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
	addrList []oid.Address
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
func (r ListWithCursorRes) AddressList() []oid.Address {
	return r.addrList
}

// Cursor returns cursor for consecutive listing requests.
func (r ListWithCursorRes) Cursor() *Cursor {
	return r.cursor
}

// List returns all objects physically stored in the Shard.
func (s *Shard) List() (res SelectRes, err error) {
	lst, err := s.metaBase.Containers()
	if err != nil {
		return res, fmt.Errorf("can't list stored containers: %w", err)
	}

	filters := object.NewSearchFilters()
	filters.AddPhyFilter()

	for i := range lst {
		ids, err := meta.Select(s.metaBase, lst[i], filters) // consider making List in metabase
		if err != nil {
			s.log.Debug("can't select all objects",
				zap.Stringer("cid", lst[i]),
				zap.String("error", err.Error()))

			continue
		}

		res.addrList = append(res.addrList, ids...)
	}

	return res, nil
}

func (s *Shard) ListContainers(_ ListContainersPrm) (ListContainersRes, error) {
	containers, err := s.metaBase.Containers()
	if err != nil {
		return ListContainersRes{}, fmt.Errorf("could not get list of containers: %w", err)
	}

	return ListContainersRes{
		containers: containers,
	}, nil
}

func ListContainers(s *Shard) ([]cid.ID, error) {
	res, err := s.ListContainers(ListContainersPrm{})
	if err != nil {
		return nil, err
	}

	return res.Containers(), nil
}

// ListWithCursor lists physical objects available in shard starting from
// cursor. Includes regular, tombstone and storage group objects. Does not
// include inhumed objects. Use cursor value from response for consecutive requests.
//
// Returns ErrEndOfListing if there are no more objects to return or count
// parameter set to zero.
func (s *Shard) ListWithCursor(prm ListWithCursorPrm) (ListWithCursorRes, error) {
	var metaPrm meta.ListPrm
	metaPrm.WithCount(prm.count)
	metaPrm.WithCursor(prm.cursor)
	res, err := s.metaBase.ListWithCursor(metaPrm)
	if err != nil {
		return ListWithCursorRes{}, fmt.Errorf("could not get list of objects: %w", err)
	}

	return ListWithCursorRes{
		addrList: res.AddressList(),
		cursor:   res.Cursor(),
	}, nil
}

// ListWithCursor lists physical objects available in shard starting from
// cursor. Includes regular, tombstone and storage group objects. Does not
// include inhumed objects. Use cursor value from response for consecutive requests.
//
// Returns ErrEndOfListing if there are no more objects to return or count
// parameter set to zero.
func ListWithCursor(s *Shard, count uint32, cursor *Cursor) ([]oid.Address, *Cursor, error) {
	var prm ListWithCursorPrm
	prm.WithCount(count)
	prm.WithCursor(cursor)
	res, err := s.ListWithCursor(prm)
	if err != nil {
		return nil, nil, err
	}

	return res.AddressList(), res.Cursor(), nil
}
