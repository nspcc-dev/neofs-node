package meta

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.etcd.io/bbolt"
)

// Grave represents descriptor of DB's graveyard item.
type Grave struct {
	gcMark bool

	addr *object.Address
}

// WithGCMark returns true if grave marked for GC to be removed.
func (g *Grave) WithGCMark() bool {
	return g.gcMark
}

// Address returns buried object address.
func (g *Grave) Address() *object.Address {
	return g.addr
}

// GraveHandler is a Grave handling function.
type GraveHandler func(*Grave) error

// IterateOverGraveyard iterates over all graves in DB.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateOverGraveyard(h GraveHandler) error {
	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateOverGraveyard(tx, h)
	})
}

func (db *DB) iterateOverGraveyard(tx *bbolt.Tx, h GraveHandler) error {
	// get graveyard bucket
	bktGraveyard := tx.Bucket(graveyardBucketName)
	if bktGraveyard == nil {
		return nil
	}

	// iterate over all graves
	err := bktGraveyard.ForEach(func(k, v []byte) error {
		// parse Grave
		g, err := graveFromKV(k, v)
		if err != nil {
			return fmt.Errorf("could not parse Grave: %w", err)
		}

		// handler Grave
		return h(g)
	})

	if errors.Is(err, ErrInterruptIterator) {
		err = nil
	}

	return err
}

func graveFromKV(k, v []byte) (*Grave, error) {
	addr, err := addressFromKey(k)
	if err != nil {
		return nil, fmt.Errorf("could not parse address: %w", err)
	}

	return &Grave{
		gcMark: bytes.Equal(v, []byte(inhumeGCMarkValue)),
		addr:   addr,
	}, nil
}
