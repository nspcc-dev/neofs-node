package meta

import (
	"bytes"
	"errors"
	"fmt"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
	"golang.org/x/exp/maps"
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

// GarbageIterationPrm groups parameters of the garbage
// iteration process.
type GarbageIterationPrm struct {
	h      GarbageHandler
	offset *oid.Address
}

// SetHandler sets a handler that will be called on every
// GarbageObject.
func (g *GarbageIterationPrm) SetHandler(h GarbageHandler) {
	g.h = h
}

// SetOffset sets an offset of the iteration operation.
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
func (g *GarbageIterationPrm) SetOffset(offset oid.Address) {
	g.offset = &offset
}

// IterateOverGarbage iterates over all objects
// marked with GC mark.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateOverGarbage(p GarbageIterationPrm) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	}

	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateDeletedObj(tx, gcHandler{p.h}, p.offset)
	})
}

// TombstonedObject represents descriptor of the
// object that has been covered with tombstone.
type TombstonedObject struct {
	addr oid.Address
	tomb oid.Address
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

// TombstonedHandler is a TombstonedObject handling function.
type TombstonedHandler func(object TombstonedObject) error

// GraveyardIterationPrm groups parameters of the graveyard
// iteration process.
type GraveyardIterationPrm struct {
	h      TombstonedHandler
	offset *oid.Address
}

// SetHandler sets a handler that will be called on every
// TombstonedObject.
func (g *GraveyardIterationPrm) SetHandler(h TombstonedHandler) {
	g.h = h
}

// SetOffset sets an offset of the iteration operation.
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
func (g *GraveyardIterationPrm) SetOffset(offset oid.Address) {
	g.offset = &offset
}

// IterateOverGraveyard iterates over all graves in DB.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateOverGraveyard(p GraveyardIterationPrm) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	}

	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateDeletedObj(tx, graveyardHandler{p.h}, p.offset)
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
	if err = decodeAddressFromKey(&res.addr, k); err != nil {
		err = fmt.Errorf("decode tombstone target from key: %w", err)
	} else if err = decodeAddressFromKey(&res.tomb, v); err != nil {
		err = fmt.Errorf("decode tombstone address from value: %w", err)
	}

	return
}

// DropGraves deletes tombstoned objects from the
// graveyard bucket.
//
// Returns any error appeared during deletion process.
func (db *DB) DropGraves(tss []TombstonedObject) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	buf := make([]byte, addressKeySize)

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(graveyardBucketName)
		if bkt == nil {
			return nil
		}

		for _, ts := range tss {
			err := bkt.Delete(addressKey(ts.Address(), buf))
			if err != nil {
				return err
			}
		}

		return nil
	})
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
	initCap := reasonableLimit
	if limit < initCap {
		initCap = limit
	}

	var addrBuff oid.Address
	var cidBuff cid.ID
	var uniqueObjectsMap map[oid.ID]struct{}
	alreadyHandledContainers := make(map[cid.ID]struct{})
	resObjects := make([]oid.Address, 0, initCap)
	resContainers := make([]cid.ID, 0)

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		// start from the deleted containers since
		// an object can be deleted manually but
		// also be deleted as a part of non-existing
		// container so no need to handle it twice

		bkt := tx.Bucket(garbageContainersBucketName)
		c := bkt.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			err := cidBuff.Decode(k)
			if err != nil {
				return fmt.Errorf("parsing raw CID: %w", err)
			}

			// another container, clean the map
			if uniqueObjectsMap == nil {
				uniqueObjectsMap = make(map[oid.ID]struct{}, initCap)
			} else {
				maps.Clear(uniqueObjectsMap)
			}

			err = listContainerObjects(tx, cidBuff, uniqueObjectsMap, limit-len(resObjects))
			if err != nil {
				return fmt.Errorf("listing objects for %s container: %w", cidBuff, err)
			}

			addrBuff.SetContainer(cidBuff)
			for obj := range uniqueObjectsMap {
				addrBuff.SetObject(obj)
				resObjects = append(resObjects, addrBuff)
			}

			alreadyHandledContainers[cidBuff] = struct{}{}

			if len(resObjects) < limit {
				// all the objects from the container were listed,
				// container can be removed
				resContainers = append(resContainers, cidBuff)
			} else {
				return nil
			}
		}

		// deleted containers are not enough to reach the limit,
		// check manually deleted objects then

		bkt = tx.Bucket(garbageObjectsBucketName)
		c = bkt.Cursor()

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
