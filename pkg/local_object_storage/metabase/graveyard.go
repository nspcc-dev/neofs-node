package meta

import (
	"bytes"
	"fmt"

	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

// GarbageObject represents descriptor of the
// object that has been marked with GC.
type GarbageObject struct {
	addr *addressSDK.Address
}

// Address returns garbage object address.
func (g GarbageObject) Address() *addressSDK.Address {
	return g.addr
}

// GarbageHandler is a GarbageObject handling function.
type GarbageHandler func(GarbageObject) error

// GarbageIterationPrm groups parameters of the garbage
// iteration process.
type GarbageIterationPrm struct {
	h      GarbageHandler
	offset *addressSDK.Address
}

// SetHandler sets a handler that will be called on every
// GarbageObject.
func (g *GarbageIterationPrm) SetHandler(h GarbageHandler) *GarbageIterationPrm {
	g.h = h
	return g
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
func (g *GarbageIterationPrm) SetOffset(offset *addressSDK.Address) *GarbageIterationPrm {
	g.offset = offset
	return g
}

// IterateOverGarbage iterates over all objects
// marked with GC mark.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateOverGarbage(p *GarbageIterationPrm) error {
	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateDeletedObj(tx, gcHandler{p.h}, p.offset)
	})
}

// TombstonedObject represents descriptor of the
// object that has been covered with tombstone.
type TombstonedObject struct {
	addr *addressSDK.Address
	tomb *addressSDK.Address
}

// Address returns tombstoned object address.
func (g TombstonedObject) Address() *addressSDK.Address {
	return g.addr
}

// Tombstone returns address of a tombstone that
// covers object.
func (g TombstonedObject) Tombstone() *addressSDK.Address {
	return g.tomb
}

// TombstonedHandler is a TombstonedObject handling function.
type TombstonedHandler func(object TombstonedObject) error

// GraveyardIterationPrm groups parameters of the graveyard
// iteration process.
type GraveyardIterationPrm struct {
	h      TombstonedHandler
	offset *addressSDK.Address
}

// SetHandler sets a handler that will be called on every
// TombstonedObject.
func (g *GraveyardIterationPrm) SetHandler(h TombstonedHandler) *GraveyardIterationPrm {
	g.h = h
	return g
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
func (g *GraveyardIterationPrm) SetOffset(offset *addressSDK.Address) *GraveyardIterationPrm {
	g.offset = offset
	return g
}

// IterateOverGraveyard iterates over all graves in DB.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateOverGraveyard(p *GraveyardIterationPrm) error {
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

func (db *DB) iterateDeletedObj(tx *bbolt.Tx, h kvHandler, offset *addressSDK.Address) error {
	var bkt *bbolt.Bucket
	switch t := h.(type) {
	case graveyardHandler:
		bkt = tx.Bucket(graveyardBucketName)
	case gcHandler:
		bkt = tx.Bucket(garbageBucketName)
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
		rawAddr := addressKey(offset)

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

func garbageFromKV(k []byte) (GarbageObject, error) {
	addr, err := addressFromKey(k)
	if err != nil {
		return GarbageObject{}, fmt.Errorf("could not parse address: %w", err)
	}

	return GarbageObject{
		addr: addr,
	}, nil
}

func graveFromKV(k, v []byte) (TombstonedObject, error) {
	target, err := addressFromKey(k)
	if err != nil {
		return TombstonedObject{}, fmt.Errorf("could not parse address: %w", err)
	}

	tomb, err := addressFromKey(v)
	if err != nil {
		return TombstonedObject{}, err
	}

	return TombstonedObject{
		addr: target,
		tomb: tomb,
	}, nil
}
