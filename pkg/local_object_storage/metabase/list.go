package meta

import (
	"bytes"

	"github.com/nspcc-dev/bbolt"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
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
	inBucketOffset oid.ID
	attrsPrefix    []byte
}

// ListWithCursor lists physical objects available in metabase starting from
// cursor. Includes objects of all types. Does not include inhumed objects.
// Use cursor value from response for consecutive requests.
//
// Optional attrs specifies attributes to include in the result. If object does
// not have requested attribute, corresponding element in the result is empty.
//
// Returns ErrEndOfListing if there are no more objects to return or count
// parameter set to zero.
func (db *DB) ListWithCursor(count int, cursor *Cursor, attrs ...string) ([]objectcore.AddressWithAttributes, *Cursor, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, nil, ErrDegradedMode
	}

	var (
		err    error
		result = make([]objectcore.AddressWithAttributes, 0, count)
	)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		result, cursor, err = db.listWithCursor(tx, result, count, cursor, attrs...)
		return err
	})

	return result, cursor, err
}

func (db *DB) listWithCursor(tx *bbolt.Tx, result []objectcore.AddressWithAttributes, count int, cursor *Cursor, attrs ...string) ([]objectcore.AddressWithAttributes, *Cursor, error) {
	var haveCur = cursor != nil
	var bucketName []byte

	c := tx.Cursor()
	name, _ := c.First()

	if haveCur {
		name, _ = c.Seek(cursor.bucketName)
	}

loop:
	for ; name != nil; name, _ = c.Next() {
		containerID, prefix := parseContainerIDWithPrefix(name)
		if containerID.IsZero() || prefix != metadataPrefix {
			continue
		}

		bkt := tx.Bucket(name)
		if bkt != nil {
			if !haveCur && cursor != nil {
				cursor.inBucketOffset = oid.ID{} // Reset for the next bucket.
			}
			result, cursor = selectNFromBucket(bkt, containerID,
				result, count, cursor, attrs...)
		}
		bucketName = name
		if len(result) >= count {
			break loop
		}
		haveCur = false
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
	cnt cid.ID, // container ID
	to []objectcore.AddressWithAttributes, // listing result
	limit int, // stop listing at `limit` items in result
	cursor *Cursor, // start from cursor object
	attrs ...string,
) ([]objectcore.AddressWithAttributes, *Cursor) {
	if cursor == nil {
		cursor = new(Cursor)
	}

	var (
		c          = bkt.Cursor()
		count      = len(to)
		typePrefix = make([]byte, metaIDTypePrefixSize)
		gotAttrs   []string
	)

	if containerMarkedGC(c) {
		return to, cursor
	}

	fillIDTypePrefix(typePrefix)

	for obj := range iterPrefixedIDs(c, mkFilterPhysicalPrefix(), cursor.inBucketOffset) {
		if count >= limit {
			break
		}

		mCursor := bkt.Cursor()
		cursor.inBucketOffset = obj
		if inGarbage(mCursor, obj) != statusAvailable {
			continue
		}

		objType, err := fetchTypeForIDWBuf(mCursor, typePrefix, obj)
		if err != nil {
			continue
		}

		if len(attrs) > 0 {
			if cursor.attrsPrefix == nil {
				mx := islices.MaxLen(attrs)
				cursor.attrsPrefix = make([]byte, attrIDFixedLen+mx)
			}

			gotAttrs = make([]string, len(attrs))
			for i := range attrs {
				n := fillIDAttributePrefix(cursor.attrsPrefix, obj, attrs[i])
				if k, _ := mCursor.Seek(cursor.attrsPrefix[:n]); bytes.HasPrefix(k, cursor.attrsPrefix[:n]) {
					gotAttrs[i] = string(k[n:])
				}
			}
		}

		var a oid.Address
		a.SetContainer(cnt)
		a.SetObject(obj)
		to = append(to, objectcore.AddressWithAttributes{Address: a, Type: objType, Attributes: gotAttrs})
		count++
	}

	return to, cursor
}

func parseContainerIDWithPrefix(name []byte) (cid.ID, byte) {
	if len(name) < bucketKeySize {
		return cid.ID{}, 0
	}

	var id cid.ID

	if err := id.Decode(name[1:bucketKeySize]); err != nil {
		return cid.ID{}, 0
	}

	return id, name[0]
}
