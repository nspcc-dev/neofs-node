package engine

import (
	"sort"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
)

// ErrEndOfListing is returned from an object listing with cursor
// when the storage can't return any more objects after the provided
// cursor. Use nil cursor object to start listing again.
var ErrEndOfListing = shard.ErrEndOfListing

// Cursor is a type for continuous object listing.
type Cursor struct {
	shardID     string
	shardCursor *shard.Cursor
}

// ListWithCursorPrm contains parameters for ListWithCursor operation.
type ListWithCursorPrm struct {
	count  uint32
	cursor *Cursor
}

// WithCount sets the maximum amount of addresses that ListWithCursor should return.
func (p *ListWithCursorPrm) WithCount(count uint32) {
	p.count = count
}

// WithCursor sets a cursor for ListWithCursor operation. For initial request
// ignore this param or use nil value. For consecutive requests, use value
// from ListWithCursorRes.
func (p *ListWithCursorPrm) WithCursor(cursor *Cursor) {
	p.cursor = cursor
}

// ListWithCursorRes contains values returned from ListWithCursor operation.
type ListWithCursorRes struct {
	addrList []objectcore.AddressWithType
	cursor   *Cursor
}

// AddressList returns addresses selected by ListWithCursor operation.
func (l ListWithCursorRes) AddressList() []objectcore.AddressWithType {
	return l.addrList
}

// Cursor returns cursor for consecutive listing requests.
func (l ListWithCursorRes) Cursor() *Cursor {
	return l.cursor
}

// ListWithCursor lists physical objects available in the engine starting
// from the cursor. It includes regular, tombstone and storage group objects.
// Does not include inhumed objects. Use cursor value from the response
// for consecutive requests.
//
// Returns ErrEndOfListing if there are no more objects to return or count
// parameter set to zero.
func (e *StorageEngine) ListWithCursor(prm ListWithCursorPrm) (ListWithCursorRes, error) {
	result := make([]objectcore.AddressWithType, 0, prm.count)

	// 1. Get available shards and sort them.
	e.mtx.RLock()
	shardIDs := make([]string, 0, len(e.shards))
	for id := range e.shards {
		shardIDs = append(shardIDs, id)
	}
	e.mtx.RUnlock()

	if len(shardIDs) == 0 {
		return ListWithCursorRes{}, ErrEndOfListing
	}

	sort.Slice(shardIDs, func(i, j int) bool {
		return shardIDs[i] < shardIDs[j]
	})

	// 2. Prepare cursor object.
	cursor := prm.cursor
	if cursor == nil {
		cursor = &Cursor{shardID: shardIDs[0]}
	}

	// 3. Iterate over available shards. Skip unavailable shards.
	for i := range shardIDs {
		if len(result) >= int(prm.count) {
			break
		}

		if shardIDs[i] < cursor.shardID {
			continue
		}

		e.mtx.RLock()
		shardInstance, ok := e.shards[shardIDs[i]]
		e.mtx.RUnlock()
		if !ok {
			continue
		}

		count := uint32(int(prm.count) - len(result))
		var shardPrm shard.ListWithCursorPrm
		shardPrm.WithCount(count)
		if shardIDs[i] == cursor.shardID {
			shardPrm.WithCursor(cursor.shardCursor)
		}

		res, err := shardInstance.ListWithCursor(shardPrm)
		if err != nil {
			continue
		}

		result = append(result, res.AddressList()...)
		cursor.shardCursor = res.Cursor()
		cursor.shardID = shardIDs[i]
	}

	if len(result) == 0 {
		return ListWithCursorRes{}, ErrEndOfListing
	}

	return ListWithCursorRes{
		addrList: result,
		cursor:   cursor,
	}, nil
}
