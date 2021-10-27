package engine

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
)

// ListWithCursorPrm contains parameters for ListWithCursor operation.
type ListWithCursorPrm struct {
	count  uint32
	cursor string
	shard  shard.ID
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

// WithShardID sets shard where listing will process.
func (p *ListWithCursorPrm) WithShardID(id shard.ID) *ListWithCursorPrm {
	p.shard = id
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
	e.mtx.RLock()
	shardInstance, ok := e.shards[prm.shard.String()]
	e.mtx.RUnlock()

	if !ok {
		return nil, errShardNotFound
	}

	shardPrm := new(shard.ListWithCursorPrm).WithCursor(prm.cursor).WithCount(prm.count)
	res, err := shardInstance.ListWithCursor(shardPrm)
	if err != nil {
		return nil, err
	}

	return &ListWithCursorRes{
		addrList: res.AddressList(),
		cursor:   res.Cursor(),
	}, nil
}
