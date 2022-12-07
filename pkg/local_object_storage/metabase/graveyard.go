package meta

import (
	"bytes"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
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
func (db *DB) IterateOverGarbage(p GarbageIterationPrm) (err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	}

	return db.boltDB.View(func(tx *bbolt.Tx) (err error) {
		defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
		defer common.BboltFatalHandler(&err)

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
func (db *DB) IterateOverGraveyard(p GraveyardIterationPrm) (err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	}

	return db.boltDB.View(func(tx *bbolt.Tx) (err error) {
		defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
		defer common.BboltFatalHandler(&err)

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
		bkt = tx.Bucket(garbageBucketName)
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

	return db.boltDB.Update(func(tx *bbolt.Tx) (err error) {
		defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
		defer common.BboltFatalHandler(&err)

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
