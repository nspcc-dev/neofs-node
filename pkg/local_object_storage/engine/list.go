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
	shardID     string
	shardCursor *shard.Cursor
}

// ListWithCursor lists physical objects available in the engine starting
// from the cursor. It includes regular, tombstone and storage group objects.
// Does not include inhumed objects. Use cursor value from the response
// for consecutive requests (it's nil when iteration is over).
//
// Returns ErrEndOfListing if there are no more objects to return or count
// parameter set to zero.
func (e *StorageEngine) ListWithCursor(count uint32, cursor *Cursor) ([]objectcore.AddressWithType, *Cursor, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddListObjectsDuration)()
	}

	result := make([]objectcore.AddressWithType, 0, count)

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
		cursor = &Cursor{shardID: shardIDs[0]}
	}

	// 3. Iterate over available shards. Skip unavailable shards.
	for i := range shardIDs {
		if len(result) >= int(count) {
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

		count := uint32(int(count) - len(result))
		var shCursor *shard.Cursor
		if shardIDs[i] == cursor.shardID {
			shCursor = cursor.shardCursor
		}

		res, shCursor, err := shardInstance.ListWithCursor(int(count), shCursor)
		if err != nil {
			continue
		}

		result = append(result, res...)
		cursor.shardCursor = shCursor
		cursor.shardID = shardIDs[i]
	}

	if len(result) == 0 {
		return nil, nil, ErrEndOfListing
	}

	return result, cursor, nil
}
