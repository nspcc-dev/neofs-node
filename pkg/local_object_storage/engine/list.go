package engine

import (
	"sync"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ErrEndOfListing is returned from an object listing with cursor
// when the storage can't return any more objects after the provided
// cursor. Use nil cursor object to start listing again.
var ErrEndOfListing = shard.ErrEndOfListing

const listParallelThreshold = 4

type shardListResult struct {
	items []objectcore.AddressWithAttributes
	err   error
}

// Cursor is a type for continuous object listing. It's returned from
// [StorageEngine.ListWithCursor] and can be reused as a parameter for it for
// subsequent requests.
type Cursor struct {
	shardCursor *shard.Cursor
}

// NewCursor creates a Cursor positioned at the given container and object.
// The next call to [StorageEngine.ListWithCursor] will return objects strictly
// after this address.
func NewCursor(cnr cid.ID, obj oid.ID) *Cursor {
	return &Cursor{shardCursor: shard.NewCursor(cnr, obj)}
}

// ContainerID returns the container ID stored in the cursor.
func (c *Cursor) ContainerID() cid.ID {
	return c.shardCursor.ContainerID()
}

// ObjectID returns the object ID stored in the cursor.
func (c *Cursor) ObjectID() oid.ID {
	return c.shardCursor.LastObjectID()
}

// ListWithCursor lists physical objects available in the engine starting
// from the cursor. It includes regular, tombstone and storage group objects.
// Does not include inhumed objects. Use cursor value from the response
// for consecutive requests (it's nil when iteration is over).
//
// Objects present on multiple shards are deduplicated: each unique address
// appears exactly once, with [objectcore.AddressWithAttributes.ShardIDs]
// containing the IDs of all shards that hold it.
//
// Optional attrs specifies attributes to include in the result. If object does
// not have requested attribute, corresponding element in the result is empty.
//
// Returns ErrEndOfListing if there are no more objects to return or count
// parameter set to zero.
func (e *StorageEngine) ListWithCursor(count uint32, cursor *Cursor, attrs ...string) ([]objectcore.AddressWithAttributes, *Cursor, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddListObjectsDuration)()
	}

	if cursor == nil {
		cursor = &Cursor{shardCursor: new(shard.Cursor)}
	}

	shards := e.unsortedShards()
	var result, buf []objectcore.AddressWithAttributes

	cnr, obj := cursor.shardCursor.ContainerID(), cursor.shardCursor.LastObjectID()
	var shardResults []shardListResult
	if len(shards) <= listParallelThreshold {
		shardResults = listSequential(shards, int(count), cnr, obj, attrs...)
	} else {
		shardResults = listParallel(shards, int(count), cnr, obj, attrs...)
	}

	for i := range shards {
		if shardResults[i].err != nil || len(shardResults[i].items) == 0 {
			continue
		}
		prev := result
		result = mergeListResults(buf, result, shardResults[i].items, shards[i].ID().String(), int(count))
		if prev != nil {
			buf = prev[:0]
		}
	}

	if len(result) == 0 {
		return nil, nil, ErrEndOfListing
	}

	last := result[len(result)-1]
	cursor.shardCursor.Reset(last.Address.Container(), last.Address.Object())
	return result, cursor, nil
}

func listSequential(shards []shardWrapper, count int, cnr cid.ID, obj oid.ID, attrs ...string) []shardListResult {
	res := make([]shardListResult, len(shards))
	crs := shard.NewCursor(cnr, obj)
	for i := range shards {
		crs.Reset(cnr, obj)
		res[i].items, _, res[i].err = shards[i].ListWithCursor(count, crs, attrs...)
	}
	return res
}

func listParallel(shards []shardWrapper, count int, cnr cid.ID, obj oid.ID, attrs ...string) []shardListResult {
	res := make([]shardListResult, len(shards))
	workers := (len(shards) + listParallelThreshold - 1) / listParallelThreshold
	chunkSize := (len(shards) + workers - 1) / workers

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := range workers {
		start := w * chunkSize
		end := start + chunkSize
		end = min(end, len(shards))

		go func(start, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				crs := shard.NewCursor(cnr, obj)
				res[i].items, _, res[i].err = shards[i].ListWithCursor(count, crs, attrs...)
			}
		}(start, end)
	}

	wg.Wait()
	return res
}

// mergeListResults merges a sorted accumulated result with a new sorted slice
// of items from a single shard into a single sorted deduplicated slice of at
// most count items. Objects present on multiple shards have their ShardIDs merged.
func mergeListResults(out, a, b []objectcore.AddressWithAttributes, shardID string, count int) []objectcore.AddressWithAttributes {
	if len(a) == 0 {
		end := min(count, len(b))
		for i := range end {
			b[i].ShardIDs = []string{shardID}
		}
		return b[:end]
	}
	if out == nil {
		out = make([]objectcore.AddressWithAttributes, 0, min(count, len(a)+len(b)))
	}
	i, j := 0, 0
	for len(out) < count && (i < len(a) || j < len(b)) {
		var cmp int
		if i >= len(a) {
			cmp = 1
		} else if j >= len(b) {
			cmp = -1
		} else {
			cmp = a[i].Address.Compare(b[j].Address)
		}

		if cmp > 0 {
			item := b[j]
			item.ShardIDs = []string{shardID}
			out = append(out, item)
			j++
		} else {
			item := a[i]
			if cmp == 0 {
				item.ShardIDs = append(item.ShardIDs, shardID)
				j++
			}
			out = append(out, item)
			i++
		}
	}
	return out
}
