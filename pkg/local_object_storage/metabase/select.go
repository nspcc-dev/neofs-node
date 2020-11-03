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

		if len(fs) == 0 {
			// get primary bucket
			primaryBucket := tx.Bucket(primaryBucket)
			if primaryBucket == nil {
				// empty storage
				return nil
			}

			// iterate over all stored addresses
			return primaryBucket.ForEach(func(k, v []byte) error {
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
		}

		// keep processed addresses
		// value equal to number (index+1) of latest matched filter
		mAddr := make(map[string]int)

		for fNum := range fs {
			matchFunc, ok := db.matchers[fs[fNum].Operation()]
			if !ok {
				return errors.Errorf("no function for matcher %v", fs[fNum].Operation())
			}

			key := fs[fNum].Header()

			// get bucket with values
			keyBucket := indexBucket.Bucket([]byte(key))
			if keyBucket == nil {
				// no object has this attribute => empty result
				return nil
			}

			fVal := fs[fNum].Value()

			// iterate over all existing values for the key
			if err := keyBucket.ForEach(func(k, v []byte) error {
				include := matchFunc(string(key), string(cutKeyBytes(k)), fVal)

				strs, err := decodeAddressList(v)
				if err != nil {
					return errors.Wrapf(err, "(%T) could not decode address list", db)
				}

				for j := range strs {
					if num := mAddr[strs[j]]; num != fNum {
						// than object does not match some previous filter
						continue
					} else if include {
						mAddr[strs[j]] = fNum + 1
					}
				}

				return nil
			}); err != nil {
				return errors.Wrapf(err, "(%T) could not iterate bucket %s", db, key)
			}
		}

		fLen := len(fs)

		for a, ind := range mAddr {
			if ind != fLen {
				continue
			}

			// check if object marked as deleted
			if objectRemoved(tx, []byte(a)) {
				continue
			}

			addr := object.NewAddress()
			if err := addr.Parse(a); err != nil {
				// TODO: storage was broken, so we need to handle it
				return err
			}

			res = append(res, addr)
		}

		return nil
	})

	return res, err
}
