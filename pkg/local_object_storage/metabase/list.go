package meta

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	core "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"go.etcd.io/bbolt"
)

// Cursor is a type for continuous object listing.
type Cursor struct {
	bucket  uint8
	address *object.Address
}

// ListPrm contains parameters for ListWithCursor operation.
type ListPrm struct {
	count  int
	cursor *Cursor
}

// WithCount sets maximum amount of addresses that ListWithCursor should return.
func (l *ListPrm) WithCount(count uint32) *ListPrm {
	l.count = int(count)
	return l
}

// WithCursor sets cursor for ListWithCursor operation. For initial request
// ignore this param or use nil value. For consecutive requests, use value
// from ListRes.
func (l *ListPrm) WithCursor(cursor *Cursor) *ListPrm {
	l.cursor = cursor
	return l
}

// ListRes contains values returned from ListWithCursor operation.
type ListRes struct {
	addrList []*object.Address
	cursor   *Cursor
}

// AddressList returns addresses selected by ListWithCursor operation.
func (l ListRes) AddressList() []*object.Address {
	return l.addrList
}

// Cursor returns cursor for consecutive listing requests.
func (l ListRes) Cursor() *Cursor {
	return l.cursor
}

const (
	cursorBucketPrimary = iota
	cursorBucketTombstone
	cursorBucketSG
)

// ListWithCursor lists physical objects available in metabase starting from
// cursor. Includes regular, tombstone and storage group objects. Does not
// include inhumed objects. Use cursor value from response for consecutive requests.
//
// Returns ErrEndOfListing if there are no more objects to return or count
// parameter set to zero.
func ListWithCursor(db *DB, count uint32, cursor *Cursor) ([]*object.Address, *Cursor, error) {
	r, err := db.ListWithCursor(new(ListPrm).WithCount(count).WithCursor(cursor))
	if err != nil {
		return nil, nil, err
	}

	return r.AddressList(), r.Cursor(), nil
}

// ListWithCursor lists physical objects available in metabase starting from
// cursor. Includes regular, tombstone and storage group objects. Does not
// include inhumed objects. Use cursor value from response for consecutive requests.
//
// Returns ErrEndOfListing if there are no more objects to return or count
// parameter set to zero.
func (db *DB) ListWithCursor(prm *ListPrm) (res *ListRes, err error) {
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res = new(ListRes)
		res.addrList, res.cursor, err = db.listWithCursor(tx, prm.count, prm.cursor)
		return err
	})

	return res, err
}

func (db *DB) listWithCursor(tx *bbolt.Tx, count int, cursor *Cursor) ([]*object.Address, *Cursor, error) {
	threshold := cursor == nil // threshold is a flag to ignore cursor
	result := make([]*object.Address, 0, count)
	unique := make(map[string]struct{}) // do not parse the same containerID twice

	c := tx.Cursor()
	name, _ := c.First()

	if !threshold {
		name, _ = c.Seek([]byte(cursor.address.ContainerID().String()))
	}

loop:
	for ; name != nil; name, _ = c.Next() {
		containerID := parseContainerID(name, unique)
		if containerID == nil {
			continue
		}

		unique[containerID.String()] = struct{}{}
		prefix := containerID.String() + "/"

		lookupBuckets := [...]struct {
			name   []byte
			bucket uint8
		}{
			{primaryBucketName(containerID), cursorBucketPrimary},
			{tombstoneBucketName(containerID), cursorBucketTombstone},
			{storageGroupBucketName(containerID), cursorBucketSG},
		}

		for _, lb := range lookupBuckets {
			if !threshold && cursor.bucket != lb.bucket {
				continue // start from the bucket, specified in the cursor bucket
			}

			result, cursor = selectNFromBucket(tx, lb.name, prefix, result, count, cursor, threshold)
			cursor.bucket = lb.bucket
			if len(result) >= count {
				break loop
			}

			// set threshold flag after first `selectNFromBucket` invocation
			// first invocation must look for cursor object
			threshold = true
		}
	}

	if len(result) == 0 {
		return nil, nil, core.ErrEndOfListing
	}

	return result, cursor, nil
}

// selectNFromBucket similar to selectAllFromBucket but uses cursor to find
// object to start selecting from. Ignores inhumed objects.
func selectNFromBucket(tx *bbolt.Tx,
	name []byte, // bucket name
	prefix string, // string of CID, optimization
	to []*object.Address, // listing result
	limit int, // stop listing at `limit` items in result
	cursor *Cursor, // start from cursor object
	threshold bool, // ignore cursor and start immediately
) ([]*object.Address, *Cursor) {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return to, cursor
	}

	if cursor == nil {
		cursor = new(Cursor)
	}

	count := len(to)
	c := bkt.Cursor()
	k, _ := c.First()

	if !threshold {
		c.Seek([]byte(cursor.address.ObjectID().String()))
		k, _ = c.Next() // we are looking for objects _after_ the cursor
	}

	for ; k != nil; k, _ = c.Next() {
		if count >= limit {
			break
		}
		a := object.NewAddress()
		if err := a.Parse(prefix + string(k)); err != nil {
			break
		}
		cursor.address = a
		if inGraveyard(tx, a) > 0 {
			continue
		}
		to = append(to, a)
		count++
	}

	return to, cursor
}
