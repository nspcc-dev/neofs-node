package meta

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

// Select returns list of addresses of objects that match search filters.
func (db *DB) Select(fs object.SearchFilters) ([]*object.Address, error) {
	res := make([]*object.Address, 0)

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		// get indexed bucket
		indexBucket := tx.Bucket(indexBucket)
		if indexBucket == nil {
			// empty storage
			return nil
		}

		// keep addresses that does not match some filter
		mAddr := make(map[string]struct{})

		for _, f := range fs {
			matchFunc, ok := db.matchers[f.Operation()]
			if !ok {
				return errors.Errorf("no function for matcher %v", f.Operation())
			}

			key := f.Header()

			// get bucket with values
			keyBucket := indexBucket.Bucket([]byte(key))
			if keyBucket == nil {
				// no object has this attribute => empty result
				return nil
			}

			fVal := f.Value()

			// iterate over all existing values for the key
			if err := keyBucket.ForEach(func(k, _ []byte) error {
				if !matchFunc(string(key), string(cutKeyBytes(k)), fVal) {
					// exclude all addresses with this value
					return keyBucket.Bucket(k).ForEach(func(k, _ []byte) error {
						mAddr[string(k)] = struct{}{}

						return nil
					})
				}

				return nil
			}); err != nil {
				return errors.Wrapf(err, "(%T) could not iterate bucket %s", db, key)
			}
		}

		// get primary bucket
		primaryBucket := tx.Bucket(primaryBucket)
		if primaryBucket == nil {
			// empty storage
			return nil
		}

		// iterate over all stored addresses
		return primaryBucket.ForEach(func(k, v []byte) error {
			if _, ok := mAddr[string(k)]; ok {
				return nil
			}

			// check if object marked as deleted
			if objectRemoved(tx, k) {
				return nil
			}

			addr := object.NewAddress()
			if err := addr.Parse(string(k)); err != nil {
				// TODO: storage was broken, so we need to handle it
				return err
			}

			res = append(res, addr)

			return nil
		})
	})

	return res, err
}
