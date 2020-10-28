package meta

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

type bucketItem struct {
	key, val string
}

var (
	primaryBucket = []byte("objects")
	indexBucket   = []byte("index")
)

// Put saves object in DB.
//
// Object payload expected to be cut.
func (db *DB) Put(obj *object.Object) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		// create primary bucket (addr: header)
		primaryBucket, err := tx.CreateBucketIfNotExists(primaryBucket)
		if err != nil {
			return errors.Wrapf(err, "(%T) could not create primary bucket", db)
		}

		data, err := obj.ToV2().StableMarshal(nil)
		if err != nil {
			return errors.Wrapf(err, "(%T) could not marshal the object", db)
		}

		addrKey := addressKey(obj.Address())

		// put header to primary bucket
		if err := primaryBucket.Put(addrKey, data); err != nil {
			return errors.Wrapf(err, "(%T) could not put item to primary bucket", db)
		}

		// create bucket for indices
		indexBucket, err := tx.CreateBucketIfNotExists(indexBucket)
		if err != nil {
			return errors.Wrapf(err, "(%T) could not create index bucket", db)
		}

		// calculate indexed values for object
		indices := objectIndices(obj)

		for i := range indices {
			// create index bucket
			keyBucket, err := indexBucket.CreateBucketIfNotExists([]byte(indices[i].key))
			if err != nil {
				return errors.Wrapf(err, "(%T) could not create bucket for header key", db)
			}

			// FIXME: here we can get empty slice that could not be the key
			// Possible solutions:
			// 1. add prefix byte (0 if empty);
			v := []byte(indices[i].val)

			// put value to key bucket (it is needed for iteration over all values (Select))
			if err := keyBucket.Put(keyWithPrefix(v, false), nil); err != nil {
				return errors.Wrapf(err, "(%T) could not put header value", db)
			}

			// create address bucket for the value
			valBucket, err := keyBucket.CreateBucketIfNotExists(keyWithPrefix(v, true))
			if err != nil {
				return errors.Wrapf(err, "(%T) could not create bucket for header value", db)
			}

			// put object address to value bucket
			if err := valBucket.Put(addrKey, nil); err != nil {
				return errors.Wrapf(err, "(%T) could not put item to header bucket", db)
			}
		}

		return nil
	})
}

func keyWithPrefix(key []byte, bucket bool) []byte {
	b := byte(0)
	if bucket {
		b = 1
	}

	return append([]byte{b}, key...)
}

func keyWithoutPrefix(key []byte) ([]byte, bool) {
	return key[1:], key[0] == 1
}

func addressKey(addr *objectSDK.Address) []byte {
	return []byte(addr.String())
}

func objectIndices(obj *object.Object) []bucketItem {
	as := obj.GetAttributes()

	res := make([]bucketItem, 0, 3+len(as))

	res = append(res,
		bucketItem{
			key: v2object.FilterHeaderVersion,
			val: obj.GetVersion().String(),
		},
		bucketItem{
			key: v2object.FilterHeaderContainerID,
			val: obj.GetContainerID().String(),
		},
		bucketItem{
			key: v2object.FilterHeaderOwnerID,
			val: obj.GetOwnerID().String(),
		},
		// TODO: add remaining fields after neofs-api#72
	)

	for _, a := range as {
		res = append(res, bucketItem{
			key: a.GetKey(),
			val: a.GetValue(),
		})
	}

	return res
}
