package meta

import (
	"bytes"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/pkg/errors"
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
// Returns errors of h directly.
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
	return bktGraveyard.ForEach(func(k, v []byte) error {
		// parse Grave
		g, err := graveFromKV(k, v)
		if err != nil {
			return errors.Wrap(err, "could not parse Grave")
		}

		// handler Grave
		return h(g)
	})
}

func graveFromKV(k, v []byte) (*Grave, error) {
	addr, err := addressFromKey(k)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse address")
	}

	return &Grave{
		gcMark: bytes.Equal(v, []byte(inhumeGCMarkValue)),
		addr:   addr,
	}, nil
}
