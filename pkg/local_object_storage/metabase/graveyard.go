package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// GarbageObject represents descriptor of the
// object that has been marked with GC.
type GarbageObject struct {
	addr oid.Address
}

// Address returns garbage object address.
func (g GarbageObject) Address() oid.Address {
	return g.addr
}

// GarbageHandler is a GarbageObject handling function.
type GarbageHandler func(GarbageObject) error

// IterateOverGarbage iterates over all objects marked with GC mark.
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
// Nil offset means start an integration from the beginning.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateOverGarbage(h GarbageHandler, offset *oid.Address) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	}

	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateDeletedObj(tx, gcHandler{h}, offset)
	})
}

// TombstonedObject represents descriptor of the
// object that has been covered with tombstone.
type TombstonedObject struct {
	addr           oid.Address
	tomb           oid.Address
	tombExpiration uint64
}

// Address returns tombstoned object address.
func (g TombstonedObject) Address() oid.Address {
	return g.addr
}

// Tombstone returns address of a tombstone that
// covers object.
func (g TombstonedObject) Tombstone() oid.Address {
	return g.tomb
}

// TombstoneExpiration returns tombstone's expiration. It can be zero if
// metabase version does not support expiration indexing or if TS does not
// expire.
func (g TombstonedObject) TombstoneExpiration() uint64 {
	return g.tombExpiration
}

// TombstonedHandler is a TombstonedObject handling function.
type TombstonedHandler func(object TombstonedObject) error

// IterateOverGraveyard iterates over all graves in DB.
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
// Nil offset means start an integration from the beginning.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateOverGraveyard(h TombstonedHandler, offset *oid.Address) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	}

	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateDeletedObj(tx, graveyardHandler{h}, offset)
	})
}

type kvHandler interface {
	handleKV(k, v []byte) error
}

type gcHandler struct {
	h GarbageHandler
}

func (g gcHandler) handleKV(k, _ []byte) error {
	o, err := garbageFromKV(k)
	if err != nil {
		return fmt.Errorf("could not parse garbage object: %w", err)
	}

	return g.h(o)
}

type graveyardHandler struct {
	h TombstonedHandler
}

func (g graveyardHandler) handleKV(k, v []byte) error {
	o, err := graveFromKV(k, v)
	if err != nil {
		return fmt.Errorf("could not parse grave: %w", err)
	}

	return g.h(o)
}

func (db *DB) iterateDeletedObj(tx *bbolt.Tx, h kvHandler, offset *oid.Address) error {
	var bkt *bbolt.Bucket
	switch t := h.(type) {
	case graveyardHandler:
		bkt = tx.Bucket(graveyardBucketName)
	case gcHandler:
		bkt = tx.Bucket(garbageObjectsBucketName)
	default:
		panic(fmt.Sprintf("metabase: unknown iteration object hadler: %T", t))
	}

	if bkt == nil {
		return nil
	}

	c := bkt.Cursor()
	var k, v []byte

	if offset == nil {
		k, v = c.First()
	} else {
		rawAddr := addressKey(*offset, make([]byte, addressKeySize))

		k, v = c.Seek(rawAddr)
		if bytes.Equal(k, rawAddr) {
			// offset was found, move
			// cursor to the next element
			k, v = c.Next()
		}
	}

	for ; k != nil; k, v = c.Next() {
		err := h.handleKV(k, v)
		if err != nil {
			if errors.Is(err, ErrInterruptIterator) {
				return nil
			}

			return err
		}
	}

	return nil
}

func garbageFromKV(k []byte) (res GarbageObject, err error) {
	err = decodeAddressFromKey(&res.addr, k)
	if err != nil {
		err = fmt.Errorf("could not parse address: %w", err)
	}

	return
}

func graveFromKV(k, v []byte) (res TombstonedObject, err error) {
	err = decodeAddressFromKey(&res.addr, k)
	if err != nil {
		return res, fmt.Errorf("decode tombstone target from key: %w", err)
	}

	err = decodeAddressFromKey(&res.tomb, v[:addressKeySize])
	if err != nil {
		return res, fmt.Errorf("decode tombstone address from value: %w", err)
	}

	switch l := len(v); l {
	case addressKeySize:
	case addressKeySize + 8:
		res.tombExpiration = binary.LittleEndian.Uint64(v[addressKeySize:])
		return
	default:
		err = fmt.Errorf("metabase: unexpected graveyard value size: %d", l)
	}

	return
}

// DropExpiredTSMarks run through the graveyard and drops tombstone marks with
// tombstones whose expiration is _less_ than provided epoch.
// Returns number of marks dropped.
func (db *DB) DropExpiredTSMarks(epoch uint64) (int, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return 0, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return 0, ErrReadOnlyMode
	}

	var counter int

	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(graveyardBucketName)
		c := bkt.Cursor()
		k, v := c.First()

		for k != nil {
			if binary.LittleEndian.Uint64(v[addressKeySize:]) < epoch {
				err := c.Delete()
				if err != nil {
					return err
				}

				counter++

				// see https://github.com/etcd-io/bbolt/pull/614; there is not
				// much we can do with such an unfixed behavior
				k, v = c.Seek(k)
			} else {
				k, v = c.Next()
			}
		}

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("cleared %d TS marks in %d epoch and got error: %w", counter, epoch, err)
	}

	return counter, nil
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

	var addrBuff oid.Address
	alreadyHandledContainers := make(map[cid.ID]struct{})
	resObjects := make([]oid.Address, 0, initCap)
	resContainers := make([]cid.ID, 0)

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		// start from the deleted containers since
		// an object can be deleted manually but
		// also be deleted as a part of non-existing
		// container so no need to handle it twice

		var inhumedCnrs []cid.ID
		err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if name[0] == metadataPrefix && containerMarkedGC(b.Cursor()) {
				var cnr cid.ID
				cidRaw, prefix := parseContainerIDWithPrefix(&cnr, name)
				if cidRaw == nil || prefix != metadataPrefix {
					return nil
				}
				inhumedCnrs = append(inhumedCnrs, cnr)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("scanning inhumed containers: %w", err)
		}

		for _, cnr := range inhumedCnrs {
			resObjects, err = listContainerObjects(tx, cnr, resObjects, limit)
			if err != nil {
				return fmt.Errorf("listing objects for %s container: %w", cnr, err)
			}

			alreadyHandledContainers[cnr] = struct{}{}

			if len(resObjects) < limit {
				// all the objects from the container were listed,
				// container can be removed
				resContainers = append(resContainers, cnr)
			} else {
				return nil
			}
		}

		// deleted containers are not enough to reach the limit,
		// check manually deleted objects then

		bkt := tx.Bucket(garbageObjectsBucketName)
		c := bkt.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			err := decodeAddressFromKey(&addrBuff, k)
			if err != nil {
				return fmt.Errorf("parsing deleted address: %w", err)
			}

			if _, handled := alreadyHandledContainers[addrBuff.Container()]; handled {
				continue
			}

			resObjects = append(resObjects, addrBuff)

			if len(resObjects) >= limit {
				return nil
			}
		}

		return nil
	})

	return resObjects, resContainers, err
}
