package meta

import (
	"errors"
	"fmt"

	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.etcd.io/bbolt"
)

// DeletedObject represents descriptor of the object that was
// marked to be deleted.
type DeletedObject struct {
	addr *addressSDK.Address
}

// Address returns buried object address.
func (g *DeletedObject) Address() *addressSDK.Address {
	return g.addr
}

// Handler is a DeletedObject handling function.
type Handler func(*DeletedObject) error

// IterateOverGarbage iterates over all objects
// marked with GC mark.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateOverGarbage(h Handler) error {
	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateDeletedObj(tx, withGC, h)
	})
}

// IterateOverGraveyard iterates over all graves in DB.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateOverGraveyard(h Handler) error {
	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateDeletedObj(tx, grave, h)
	})
}

type deletedType uint8

const (
	_ deletedType = iota
	grave
	withGC
)

func (db *DB) iterateDeletedObj(tx *bbolt.Tx, t deletedType, h Handler) error {
	var bkt *bbolt.Bucket
	switch t {
	case grave:
		bkt = tx.Bucket(graveyardBucketName)
	case withGC:
		bkt = tx.Bucket(garbageBucketName)
	default:
		panic(fmt.Sprintf("metabase: unknown iteration object type: %d", t))
	}

	if bkt == nil {
		return nil
	}

	// iterate over all deleted objects
	err := bkt.ForEach(func(k, v []byte) error {
		// parse deleted object
		delObj, err := deletedObjectFromKV(k, v)
		if err != nil {
			return fmt.Errorf("could not parse Grave: %w", err)
		}

		// handler object
		return h(delObj)
	})

	if errors.Is(err, ErrInterruptIterator) {
		err = nil
	}

	return err
}

func deletedObjectFromKV(k, _ []byte) (*DeletedObject, error) {
	addr, err := addressFromKey(k)
	if err != nil {
		return nil, fmt.Errorf("could not parse address: %w", err)
	}

	return &DeletedObject{
		addr: addr,
	}, nil
}
