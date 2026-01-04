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
	containerID  cid.ID
	lastObjectID oid.ID
	attrsPrefix  []byte
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
	var (
		c    = tx.Cursor()
		name []byte
	)

	if cursor == nil {
		cursor = new(Cursor)
		name, _ = c.Seek([]byte{metadataPrefix})
	} else {
		name, _ = c.Seek(metaBucketKey(cursor.containerID))
	}

	for ; name != nil; name, _ = c.Next() {
		containerID, prefix := parseContainerIDWithPrefix(name)
		if containerID.IsZero() || prefix != metadataPrefix {
			continue
		}

		if containerID != cursor.containerID {
			cursor.lastObjectID = oid.ID{} // Reset for the next bucket.
		}
		cursor.containerID = containerID
		bkt := tx.Bucket(name)
		if bkt != nil {
			result, cursor = selectNFromBucket(bkt, result, count, cursor, attrs...)
		}
		if len(result) >= count {
			break
		}
	}

	if len(result) == 0 {
		return nil, nil, ErrEndOfListing
	}

	return result, cursor, nil
}

// selectNFromBucket similar to selectAllFromBucket but uses cursor to find
// object to start selecting from. Ignores inhumed objects.
func selectNFromBucket(bkt *bbolt.Bucket, // main bucket
	to []objectcore.AddressWithAttributes, // listing result
	limit int, // stop listing at `limit` items in result
	cursor *Cursor, // start from cursor object
	attrs ...string,
) ([]objectcore.AddressWithAttributes, *Cursor) {
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

	for obj := range iterPrefixedIDs(c, mkFilterPhysicalPrefix(), cursor.lastObjectID) {
		if count >= limit {
			break
		}

		mCursor := bkt.Cursor()
		cursor.lastObjectID = obj
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
		a.SetContainer(cursor.containerID)
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
