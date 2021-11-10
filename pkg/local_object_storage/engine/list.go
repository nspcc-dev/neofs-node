package engine

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	core "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
)

// ListWithCursorPrm contains parameters for ListWithCursor operation.
type ListWithCursorPrm struct {
	count  uint32
	cursor string
}

// WithCount sets maximum amount of addresses that ListWithCursor can return.
func (p *ListWithCursorPrm) WithCount(count uint32) *ListWithCursorPrm {
	p.count = count
	return p
}

// WithCursor sets cursor for ListWithCursor operation. For initial request
// ignore this param or use empty string. For continues requests, use  value
// from ListWithCursorRes.
func (p *ListWithCursorPrm) WithCursor(cursor string) *ListWithCursorPrm {
	p.cursor = cursor
	return p
}

// ListWithCursorRes contains values returned from ListWithCursor operation.
type ListWithCursorRes struct {
	addrList []*object.Address
	cursor   string
}

// AddressList returns addresses selected by ListWithCursor operation.
func (l ListWithCursorRes) AddressList() []*object.Address {
	return l.addrList
}

// Cursor returns cursor for consecutive listing requests.
func (l ListWithCursorRes) Cursor() string {
	return l.cursor
}

// ListWithCursor lists physical objects available in specified shard starting
// from cursor. Includes regular,tombstone and storage group objects.
// Does not include inhumed objects. Use cursor value from response
// for consecutive requests.
func (e *StorageEngine) ListWithCursor(prm *ListWithCursorPrm) (*ListWithCursorRes, error) {
	var (
		err    error
		result = make([]*object.Address, 0, prm.count)
	)

	// 1. Get available shards and sort them.
	e.mtx.RLock()
	shardIDs := make([]string, 0, len(e.shards))
	for id := range e.shards {
		shardIDs = append(shardIDs, id)
	}
	e.mtx.RUnlock()

	if len(shardIDs) == 0 {
		return nil, core.ErrEndOfListing
	}

	sort.Slice(shardIDs, func(i, j int) bool {
		return shardIDs[i] < shardIDs[j]
	})

	// 2. Decode shard ID from cursor.
	cursor := prm.cursor
	cursorShardID := shardIDs[0]
	if len(cursor) > 0 {
		cursorShardID, cursor, err = decodeID(cursor)
		if err != nil {
			return nil, err
		}
	}

	// 3. Iterate over available shards. Skip unavailable shards.
	for i := range shardIDs {
		if len(result) >= int(prm.count) {
			break
		}

		if shardIDs[i] < cursorShardID {
			continue
		}

		e.mtx.RLock()
		shardInstance, ok := e.shards[shardIDs[i]]
		e.mtx.RUnlock()
		if !ok {
			continue
		}

		count := uint32(int(prm.count) - len(result))
		shardPrm := new(shard.ListWithCursorPrm).WithCount(count)
		if shardIDs[i] == cursorShardID {
			shardPrm.WithCursor(cursor)
		}

		res, err := shardInstance.ListWithCursor(shardPrm)
		if err != nil {
			continue
		}

		result = append(result, res.AddressList()...)
		cursor = res.Cursor()
		cursorShardID = shardIDs[i]
	}

	if len(result) == 0 {
		return nil, core.ErrEndOfListing
	}

	return &ListWithCursorRes{
		addrList: result,
		cursor:   encodeID(cursorShardID, cursor),
	}, nil
}

func decodeID(cursor string) (shardID string, shardCursor string, err error) {
	ln := len(cursor)
	if ln < 2 {
		return "", "", fmt.Errorf("invalid cursor %s", cursor)
	}

	idLen, err := strconv.Atoi(cursor[:2])
	if err != nil {
		return "", "", fmt.Errorf("invalid cursor %s", cursor)
	}

	if len(cursor) < 2+idLen {
		return "", "", fmt.Errorf("invalid cursor %s", cursor)
	}

	return cursor[2 : 2+idLen], cursor[2+idLen:], nil
}

func encodeID(id, cursor string) string {
	prefix := fmt.Sprintf("%02d", len(id))
	return prefix + id + cursor
}
