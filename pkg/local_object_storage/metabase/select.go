package meta

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

// Select returns list of addresses of objects that match search filters.
func (db *DB) Select(fs object.SearchFilters) (res []*object.Address, err error) {
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res, err = db.selectObjects(tx, fs)
		return err
	})

	return
}

func (db *DB) selectObjects(tx *bbolt.Tx, fs object.SearchFilters) ([]*object.Address, error) {
	if len(fs) == 0 {
		return db.selectAll(tx)
	}

	// get indexed bucket
	indexBucket := tx.Bucket(indexBucket)
	if indexBucket == nil {
		// empty storage
		return nil, nil
	}

	// keep processed addresses
	// value equal to number (index+1) of latest matched filter
	mAddr := make(map[string]int)

	for fNum := range fs {
		matchFunc, ok := db.matchers[fs[fNum].Operation()]
		if !ok {
			return nil, errors.Errorf("no function for matcher %v", fs[fNum].Operation())
		}

		key := fs[fNum].Header()

		// get bucket with values
		keyBucket := indexBucket.Bucket([]byte(key))
		if keyBucket == nil {
			// no object has this attribute => empty result
			return nil, nil
		}

		fVal := fs[fNum].Value()

		// iterate over all existing values for the key
		if err := keyBucket.ForEach(func(k, v []byte) error {
			include := matchFunc(string(key), string(cutKeyBytes(k)), fVal)

			if include {
				return keyBucket.Bucket(k).ForEach(func(k, _ []byte) error {
					if num := mAddr[string(k)]; num == fNum {
						// otherwise object does not match current or some previous filter
						mAddr[string(k)] = fNum + 1
					}

					return nil
				})
			}

			return nil
		}); err != nil {
			return nil, errors.Wrapf(err, "(%T) could not iterate bucket %s", db, key)
		}
	}

	fLen := len(fs)
	res := make([]*object.Address, 0, len(mAddr))

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
			return nil, err
		}

		res = append(res, addr)
	}

	return res, nil
}

func (db *DB) selectAll(tx *bbolt.Tx) ([]*object.Address, error) {
	fs := object.SearchFilters{}

	// to select all objects we can select by any property
	// that splits objects into disjoint subsets
	fs.AddRootFilter()

	list1, err := db.selectObjects(tx, fs)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not select root objects", db)
	}

	fs = fs[:0]

	fs.AddNonRootFilter()

	list2, err := db.selectObjects(tx, fs)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not select non-root objects", db)
	}

	return append(list1, list2...), nil
}
