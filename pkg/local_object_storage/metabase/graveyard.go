package meta

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// TrashBin groups objects and containers that should be deleted. Empty objects
// list means container is empty and can be deleted completely.
type TrashBin struct {
	Container cid.ID
	Objects   []oid.ID
}

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
	var pref = []byte{metaPrefixGarbage}

	for obj := range iterPrefixedIDs(c, pref, offset) {
		err := h(obj)
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
// Garbage is retuned as a set of per-container TrashBins. Objects from a
// TrashBin should be removed if present. If TrashBin object list is empty
// then the respective container is empty and can be safely deleted.
func (db *DB) GetGarbage(limit int) ([]TrashBin, error) {
	return db.GetGarbageAfter(limit, cid.ID{})
}

// GetGarbageAfter is [DB.GetGarbage] that starts with the container following
// the given one in the metabase's order and comes back to the beginning when
// it reaches the end, the given container last. Calling it with the last
// container of the previous result walks all containers in turn, so that
// garbage of one of them cannot keep that of the others from being listed.
// A zero ID starts from the beginning.
func (db *DB) GetGarbageAfter(limit int, after cid.ID) ([]TrashBin, error) {
	if limit <= 0 {
		return nil, nil
	}

	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	var (
		res    []TrashBin
		objNum int
	)

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		// visit returns true when enough was collected.
		visit := func(name []byte) bool {
			cnr, prefix := parseContainerIDWithPrefix(name)
			if cnr.IsZero() || prefix != metadataPrefix {
				return false
			}
			b := tx.Bucket(name)
			var (
				cur           = b.Cursor()
				deadContainer bool
				objPrefix     = metaPrefixGarbage
			)
			if containerMarkedGC(b.Cursor()) {
				deadContainer = true
				objPrefix = metaPrefixID
			}

			objs := listGarbageObjects(cur, objPrefix, cnr, limit)
			if len(objs) != 0 {
				res = append(res, TrashBin{Container: cnr, Objects: objs})
				objNum += len(objs)
				return objNum >= limit
			}
			if deadContainer {
				// all the objects from the container were listed,
				// container can be removed
				res = append(res, TrashBin{Container: cnr})
			}
			return false
		}

		var (
			first = []byte{metadataPrefix}
			start = append([]byte{metadataPrefix}, after[:]...)
			c     = tx.Cursor()
		)
		// Containers behind the given one...
		for k, _ := c.Seek(start); k != nil; k, _ = c.Next() {
			if bytes.Equal(k, start) {
				continue
			}
			if visit(k) {
				return nil
			}
		}
		// ...then those before it, and the given one itself.
		for k, _ := c.Seek(first); k != nil && bytes.Compare(k, start) <= 0; k, _ = c.Next() {
			if visit(k) {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scanning containers: %w", err)
	}

	return res, nil
}

func listGarbageObjects(cur *bbolt.Cursor, prefix byte, cnr cid.ID, limit int) []oid.ID {
	var objs []oid.ID

	for obj := range iterPrefixedIDs(cur, []byte{prefix}, oid.ID{}) {
		if len(objs) >= limit {
			break
		}
		objs = append(objs, obj)
	}

	return objs
}
