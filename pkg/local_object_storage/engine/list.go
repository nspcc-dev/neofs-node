package engine

import (
	"slices"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
)

// ErrEndOfListing is returned from an object listing with cursor
// when the storage can't return any more objects after the provided
// cursor. Use nil cursor object to start listing again.
var ErrEndOfListing = shard.ErrEndOfListing

// Cursor is a type for continuous object listing. It's returned from
// [StorageEngine.ListWithCursor] and can be reused as a parameter for it for
// subsequent requests.
type Cursor struct {
	perShard map[string]*shard.Cursor
}

// ListWithCursor lists physical objects available in the engine starting
// from the cursor. It includes regular, tombstone and storage group objects.
// Does not include inhumed objects. Use cursor value from the response
// for consecutive requests (it's nil when iteration is over).
//
// The call queries every available shard and deduplicates the combined result
// within the call: an object present on multiple shards is emitted exactly
// once per call. Objects duplicated across shards may still appear in more
// than one call if the per-shard cursors reach the duplicate at different
// iterations. [objectcore.AddressWithAttributes.ShardIDs] is populated with the IDs of
// all local shards that returned the object in this batch.
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

	// 1. Get available shards and sort them.
	e.mtx.RLock()
	shardIDs := make([]string, 0, len(e.shards))
	for id := range e.shards {
		shardIDs = append(shardIDs, id)
	}
	e.mtx.RUnlock()

	if len(shardIDs) == 0 {
		return nil, nil, ErrEndOfListing
	}

	slices.Sort(shardIDs)

	// 2. Prepare cursor object.
	if cursor == nil {
		cursor = &Cursor{perShard: make(map[string]*shard.Cursor, len(shardIDs))}
	}

	// 3. Iterate over available shards.
	index := make(map[string]int)
	var result []objectcore.AddressWithAttributes

	for _, id := range shardIDs {
		if exhausted, known := cursor.perShard[id]; known && exhausted == nil {
			continue
		}

		e.mtx.RLock()
		shardInstance, ok := e.shards[id]
		e.mtx.RUnlock()
		if !ok {
			continue
		}

		res, shCursor, err := shardInstance.ListWithCursor(int(count), cursor.perShard[id], attrs...)
		if err != nil {
			cursor.perShard[id] = nil
			continue
		}

		cursor.perShard[id] = shCursor

		for i := range res {
			addrStr := res[i].Address.EncodeToString()
			if pos, dup := index[addrStr]; dup {
				result[pos].ShardIDs = append(result[pos].ShardIDs, id)
			} else {
				res[i].ShardIDs = []string{id}
				index[addrStr] = len(result)
				result = append(result, res[i])
			}
		}
	}

	if len(result) == 0 {
		return nil, nil, ErrEndOfListing
	}

	return result, cursor, nil
}
