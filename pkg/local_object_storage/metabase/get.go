package meta

import (
	"errors"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"go.etcd.io/bbolt"
)

var errNotFound = errors.New("object not found")

// Get returns object header for specified address.
func (db *DB) Get(addr *objectSDK.Address) (*object.Object, error) {
	var obj *object.Object

	if err := db.boltDB.View(func(tx *bbolt.Tx) error {
		addrKey := addressKey(addr)

		// check if object marked as deleted
		if objectRemoved(tx, addrKey) {
			return errNotFound
		}

		primaryBucket := tx.Bucket(primaryBucket)
		if primaryBucket == nil {
			return errNotFound
		}

		data := primaryBucket.Get(addrKey)
		if data == nil {
			return errNotFound
		}

		var err error

		obj, err = object.FromBytes(data)

		return err
	}); err != nil {
		return nil, err
	}

	return obj, nil
}
