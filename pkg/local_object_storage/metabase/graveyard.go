package meta

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
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
	return db.iterateOverGraveyard(h)
}

func (db *DB) iterateOverGraveyard(h GraveHandler) error {
	iter := db.newPrefixIterator([]byte{graveyardPrefix})
	defer iter.Close()

	var err error

	// iterate over all graves
	for iter.First(); err == nil && iter.Valid(); iter.Next() {
		var g *Grave

		// parse Grave
		g, err = graveFromKV(iter.Key()[1:], iter.Value())
		if err != nil {
			err = fmt.Errorf("could not parse Grave: %w", err)
			break
		}

		// handler Grave
		err = h(g)
	}

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
