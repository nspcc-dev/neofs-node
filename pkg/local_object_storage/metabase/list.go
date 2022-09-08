package meta

import (
	"errors"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// ErrEndOfListing is returned from object listing with cursor
// when storage can't return any more objects after provided
// cursor. Use nil cursor object to start listing again.
var ErrEndOfListing = errors.New("end of object listing")

// Cursor is a type for continuous object listing.
type Cursor struct {
	bucketName     []byte
	inBucketOffset []byte
}

// ListPrm contains parameters for ListWithCursor operation.
type ListPrm struct {
	count  int
	cursor *Cursor
}

// SetCount sets maximum amount of addresses that ListWithCursor should return.
func (l *ListPrm) SetCount(count uint32) {
	l.count = int(count)
}

// SetCursor sets cursor for ListWithCursor operation. For initial request
// ignore this param or use nil value. For consecutive requests, use value
// from ListRes.
func (l *ListPrm) SetCursor(cursor *Cursor) {
	l.cursor = cursor
}

// ListRes contains values returned from ListWithCursor operation.
type ListRes struct {
	addrList []oid.Address
	cursor   *Cursor
}

// AddressList returns addresses selected by ListWithCursor operation.
func (l ListRes) AddressList() []oid.Address {
	return l.addrList
}

// Cursor returns cursor for consecutive listing requests.
func (l ListRes) Cursor() *Cursor {
	return l.cursor
}

// ListWithCursor lists physical objects available in metabase starting from
// cursor. Includes objects of all types. Does not include inhumed objects.
// Use cursor value from response for consecutive requests.
//
// Returns ErrEndOfListing if there are no more objects to return or count
// parameter set to zero.
func (db *DB) ListWithCursor(prm ListPrm) (res ListRes, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	result := make([]oid.Address, 0, prm.count)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res.addrList, res.cursor, err = db.listWithCursor(tx, result, prm.count, prm.cursor)
		return err
	})

	return res, err
}

func (db *DB) listWithCursor(tx *bbolt.Tx, result []oid.Address, count int, cursor *Cursor) ([]oid.Address, *Cursor, error) {
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
	garbageBkt := tx.Bucket(garbageBucketName)

	var rawAddr = make([]byte, cidSize, addressKeySize)

loop:
	for ; name != nil; name, _ = c.Next() {
		cidRaw, prefix := parseContainerIDWithPrefix(&containerID, name)
		if cidRaw == nil {
			continue
		}

		switch prefix {
		case
			primaryPrefix,
			storageGroupPrefix,
			lockersPrefix,
			tombstonePrefix:
		default:
			continue
		}

		bkt := tx.Bucket(name)
		if bkt != nil {
			copy(rawAddr, cidRaw)
			result, offset, cursor = selectNFromBucket(bkt, graveyardBkt, garbageBkt, rawAddr, containerID,
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
		cursor.inBucketOffset = make([]byte, len(offset))
		copy(cursor.inBucketOffset, offset)
	}

	if len(result) == 0 {
		return nil, nil, ErrEndOfListing
	}

	// new slice is much faster but less memory efficient
	// we need to copy, because bucketName exists during bbolt tx
	cursor.bucketName = make([]byte, len(bucketName))
	copy(cursor.bucketName, bucketName)

	return result, cursor, nil
}

// selectNFromBucket similar to selectAllFromBucket but uses cursor to find
// object to start selecting from. Ignores inhumed objects.
func selectNFromBucket(bkt *bbolt.Bucket, // main bucket
	graveyardBkt, garbageBkt *bbolt.Bucket, // cached graveyard buckets
	cidRaw []byte, // container ID prefix, optimization
	cnt cid.ID, // container ID
	to []oid.Address, // listing result
	limit int, // stop listing at `limit` items in result
	cursor *Cursor, // start from cursor object
	threshold bool, // ignore cursor and start immediately
) ([]oid.Address, []byte, *Cursor) {
	if cursor == nil {
		cursor = new(Cursor)
	}

	count := len(to)
	c := bkt.Cursor()
	k, _ := c.First()

	offset := cursor.inBucketOffset

	if !threshold {
		c.Seek(offset)
		k, _ = c.Next() // we are looking for objects _after_ the cursor
	}

	for ; k != nil; k, _ = c.Next() {
		if count >= limit {
			break
		}

		var obj oid.ID
		if err := obj.Decode(k); err != nil {
			break
		}

		offset = k
		if inGraveyardWithKey(append(cidRaw, k...), graveyardBkt, garbageBkt) > 0 {
			continue
		}

		var a oid.Address
		a.SetContainer(cnt)
		a.SetObject(obj)
		to = append(to, a)
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
