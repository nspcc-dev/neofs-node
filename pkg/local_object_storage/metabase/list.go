package meta

import (
	"bytes"

	"github.com/nspcc-dev/bbolt"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ErrEndOfListing is returned from object listing with cursor
// when storage can't return any more objects after provided
// cursor. Use nil cursor object to start listing again.
var ErrEndOfListing = logicerr.New("end of object listing")

// Cursor is a type for continuous object listing.
type Cursor struct {
	bucketName     []byte
	inBucketOffset []byte
}

// ListWithCursor lists physical objects available in metabase starting from
// cursor. Includes objects of all types. Does not include inhumed objects.
// Use cursor value from response for consecutive requests.
//
// Returns ErrEndOfListing if there are no more objects to return or count
// parameter set to zero.
func (db *DB) ListWithCursor(count int, cursor *Cursor) ([]objectcore.AddressWithType, *Cursor, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, nil, ErrDegradedMode
	}

	var (
		err       error
		result    = make([]objectcore.AddressWithType, 0, count)
		currEpoch = db.epochState.CurrentEpoch()
	)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		result, cursor, err = db.listWithCursor(tx, currEpoch, result, count, cursor)
		return err
	})

	return result, cursor, err
}

func (db *DB) listWithCursor(tx *bbolt.Tx, currEpoch uint64, result []objectcore.AddressWithType, count int, cursor *Cursor) ([]objectcore.AddressWithType, *Cursor, error) {
	threshold := cursor == nil // threshold is a flag to ignore cursor
	var bucketName []byte

	c := tx.Cursor()
	name, _ := c.First()

	if !threshold {
		name, _ = c.Seek(cursor.bucketName)
	}

	var containerID cid.ID
	var offset []byte
	graveyardBkt := tx.Bucket(graveyardBucketName)
	garbageObjectsBkt := tx.Bucket(garbageObjectsBucketName)

	var rawAddr = make([]byte, cidSize, addressKeySize)

loop:
	for ; name != nil; name, _ = c.Next() {
		cidRaw, prefix := parseContainerIDWithPrefix(&containerID, name)
		if cidRaw == nil || prefix != metadataPrefix {
			continue
		}

		bkt := tx.Bucket(name)
		if bkt != nil {
			copy(rawAddr, cidRaw)
			result, offset, cursor = selectNFromBucket(bkt, currEpoch, graveyardBkt, garbageObjectsBkt, rawAddr, containerID,
				result, count, cursor, threshold)
		}
		bucketName = name
		if len(result) >= count {
			break loop
		}

		// set threshold flag after first `selectNFromBucket` invocation
		// first invocation must look for cursor object
		threshold = true
	}

	if offset != nil {
		// new slice is much faster but less memory efficient
		// we need to copy, because offset exists during bbolt tx
		cursor.inBucketOffset = bytes.Clone(offset)
	}

	if len(result) == 0 {
		return nil, nil, ErrEndOfListing
	}

	// new slice is much faster but less memory efficient
	// we need to copy, because bucketName exists during bbolt tx
	cursor.bucketName = bytes.Clone(bucketName)

	return result, cursor, nil
}

// selectNFromBucket similar to selectAllFromBucket but uses cursor to find
// object to start selecting from. Ignores inhumed objects.
func selectNFromBucket(bkt *bbolt.Bucket, // main bucket
	currEpoch uint64,
	graveyardBkt, garbageObjectsBkt *bbolt.Bucket, // cached graveyard buckets
	cidRaw []byte, // container ID prefix, optimization
	cnt cid.ID, // container ID
	to []objectcore.AddressWithType, // listing result
	limit int, // stop listing at `limit` items in result
	cursor *Cursor, // start from cursor object
	threshold bool, // ignore cursor and start immediately
) ([]objectcore.AddressWithType, []byte, *Cursor) {
	if cursor == nil {
		cursor = new(Cursor)
	}

	var (
		c          = bkt.Cursor()
		count      = len(to)
		offset     []byte
		phyPrefix  = mkFilterPhysicalPrefix()
		typePrefix = make([]byte, metaIDTypePrefixSize)
	)

	if containerMarkedGC(c) {
		return to, nil, cursor
	}

	fillIDTypePrefix(typePrefix)

	if threshold {
		offset = phyPrefix
	} else {
		offset = cursor.inBucketOffset
	}
	k, _ := c.Seek(offset)

	if !threshold {
		k, _ = c.Next() // we are looking for objects _after_ the cursor
	}

	for ; bytes.HasPrefix(k, phyPrefix); k, _ = c.Next() {
		if count >= limit {
			break
		}

		var obj oid.ID
		if err := obj.Decode(k[len(phyPrefix):]); err != nil {
			break
		}

		mCursor := bkt.Cursor()
		offset = k
		if inGraveyardWithKey(mCursor, append(cidRaw, obj[:]...), graveyardBkt, garbageObjectsBkt) != statusAvailable {
			continue
		}

		objType, err := fetchTypeForID(mCursor, typePrefix, obj)
		if err != nil {
			continue
		}

		var a oid.Address
		a.SetContainer(cnt)
		a.SetObject(obj)
		to = append(to, objectcore.AddressWithType{Address: a, Type: objType})
		count++
	}

	return to, offset, cursor
}

func parseContainerIDWithPrefix(containerID *cid.ID, name []byte) ([]byte, byte) {
	if len(name) < bucketKeySize {
		return nil, 0
	}

	rawID := name[1:bucketKeySize]

	if err := containerID.Decode(rawID); err != nil {
		return nil, 0
	}

	return rawID, name[0]
}
