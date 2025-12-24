package meta

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// IterateOverGarbage iterates over all objects marked with GC mark in
// the given container.
//
// The handler will be applied to the next after the
// specified offset if any are left.
//
// Note: if offset is not found in db, iteration starts
// from the element that WOULD BE the following after the
// offset if offset was presented. That means that it is
// safe to delete offset element and pass if to the
// iteration once again: iteration would start from the
// next element.
//
// Zero offset means start an iteration from the beginning.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateOverGarbage(h func(oid.ID) error, cnr cid.ID, offset oid.ID) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	}

	return db.boltDB.View(func(tx *bbolt.Tx) error {
		var metaBkt = tx.Bucket(metaBucketKey(cnr))
		if metaBkt == nil {
			return errors.New("no meta bucket found")
		}
		return db.iterateIDs(metaBkt.Cursor(), h, offset)
	})
}

func (db *DB) iterateIDs(c *bbolt.Cursor, h func(oid.ID) error, offset oid.ID) error {
	var (
		k    []byte
		pref = []byte{metaPrefixGarbage}
	)

	if offset.IsZero() {
		k, _ = c.Seek(pref)
	} else {
		pref = append(pref, offset[:]...)
		k, _ = c.Seek(pref)
		if bytes.Equal(k, pref) {
			k, _ = c.Next()
		}
	}

	for ; len(k) > 0 && k[0] == metaPrefixGarbage; k, _ = c.Next() {
		obj, err := oid.DecodeBytes(k[1:])
		if err != nil {
			return fmt.Errorf("garbage key of length %d: %w", len(k), err)
		}
		err = h(obj)
		if err != nil {
			if errors.Is(err, ErrInterruptIterator) {
				return nil
			}

			return err
		}
	}
	return nil
}

// GetGarbage returns garbage according to the metabase state. Garbage includes
// objects marked with GC mark (expired, tombstoned but not deleted from disk,
// extra replicated, etc.) and removed containers.
// The first return value describes garbage objects. These objects should be
// removed. The second return value describes garbage containers whose _all_
// garbage objects were included in the first return value and, therefore,
// these containers can be deleted (if their objects are handled and deleted too).
func (db *DB) GetGarbage(limit int) ([]oid.Address, []cid.ID, error) {
	if limit <= 0 {
		return nil, nil, nil
	}

	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, nil, ErrDegradedMode
	}

	const reasonableLimit = 1000
	initCap := min(limit, reasonableLimit)

	resObjects := make([]oid.Address, 0, initCap)
	resContainers := make([]cid.ID, 0)

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			var cnr cid.ID

			cidRaw, prefix := parseContainerIDWithPrefix(&cnr, name)
			if cidRaw == nil || prefix != metadataPrefix {
				return nil // continue bucket iteration
			}
			var (
				cur           = b.Cursor()
				deadContainer bool
				err           error
				objPrefix     = metaPrefixGarbage
			)
			if containerMarkedGC(b.Cursor()) {
				deadContainer = true
				objPrefix = metaPrefixID
			}

			resObjects, err = listGarbageObjects(cur, objPrefix, cnr, resObjects, limit)
			if err != nil {
				return fmt.Errorf("listing objects for %s container: %w", cnr, err)
			}
			if len(resObjects) >= limit {
				return ErrInterruptIterator
			} else if deadContainer {
				// all the objects from the container were listed,
				// container can be removed
				resContainers = append(resContainers, cnr)
			}
			return nil
		})
		if err != nil && !errors.Is(err, ErrInterruptIterator) {
			return fmt.Errorf("scanning containers: %w", err)
		}

		return nil
	})

	return resObjects, resContainers, err
}

func listGarbageObjects(cur *bbolt.Cursor, prefix byte, cnr cid.ID, objs []oid.Address, limit int) ([]oid.Address, error) {
	k, _ := cur.Seek([]byte{prefix})
	for ; len(k) > 0 && len(objs) < limit && k[0] == prefix; k, _ = cur.Next() {
		obj, err := oid.DecodeBytes(k[1:])
		if err != nil {
			return objs, fmt.Errorf("bad key of length %d with prefix %d for container %s: %w", len(k), prefix, cnr, err)
		}
		objs = append(objs, oid.NewAddress(cnr, obj))
	}

	return objs, nil
}
