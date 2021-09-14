package writecache

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"go.etcd.io/bbolt"
)

// ErrNoDefaultBucket is returned by IterateDB when default bucket for objects is missing.
var ErrNoDefaultBucket = errors.New("no default bucket")

// IterateDB iterates over all objects stored in bbolt.DB instance and passes them to f until error return.
// It is assumed that db is an underlying database of some WriteCache instance.
//
// Returns ErrNoDefaultBucket if there is no default bucket in db.
//
// DB must not be nil and should be opened.
func IterateDB(db *bbolt.DB, f func(*object.Address) error) error {
	return db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return ErrNoDefaultBucket
		}

		var addr *object.Address

		return b.ForEach(func(k, v []byte) error {
			if addr == nil {
				addr = object.NewAddress()
			}

			err := addr.Parse(string(k))
			if err != nil {
				return fmt.Errorf("could not parse object address: %w", err)
			}

			return f(addr)
		})
	})
}
