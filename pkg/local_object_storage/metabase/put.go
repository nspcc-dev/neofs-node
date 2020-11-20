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
		par := false

		for ; obj != nil; obj, par = obj.GetParent(), true {
			// create primary bucket (addr: header)
			primaryBucket, err := tx.CreateBucketIfNotExists(primaryBucket)
			if err != nil {
				return errors.Wrapf(err, "(%T) could not create primary bucket", db)
			}

			data, err := obj.Marshal()
			if err != nil {
				return errors.Wrapf(err, "(%T) could not marshal the object", db)
			}

			addrKey := addressKey(obj.Address())

			if !par {
				// put header to primary bucket
				if err := primaryBucket.Put(addrKey, data); err != nil {
					return errors.Wrapf(err, "(%T) could not put item to primary bucket", db)
				}
			}

			// create bucket for indices
			indexBucket, err := tx.CreateBucketIfNotExists(indexBucket)
			if err != nil {
				return errors.Wrapf(err, "(%T) could not create index bucket", db)
			}

			// calculate indexed values for object
			indices := objectIndices(obj, par)

			for i := range indices {
				// create index bucket
				keyBucket, err := indexBucket.CreateBucketIfNotExists([]byte(indices[i].key))
				if err != nil {
					return errors.Wrapf(err, "(%T) could not create bucket for header key", db)
				}

				v := []byte(indices[i].val)

				// create address bucket for the value
				valBucket, err := keyBucket.CreateBucketIfNotExists(nonEmptyKeyBytes(v))
				if err != nil {
					return errors.Wrapf(err, "(%T) could not create bucket for header value", db)
				}

				// put object address to value bucket
				if err := valBucket.Put(addrKey, nil); err != nil {
					return errors.Wrapf(err, "(%T) could not put item to header bucket", db)
				}
			}

		}

		return nil
	})
}

func nonEmptyKeyBytes(key []byte) []byte {
	return append([]byte{0}, key...)
}

func cutKeyBytes(key []byte) []byte {
	return key[1:]
}

func addressKey(addr *objectSDK.Address) []byte {
	return []byte(addr.String())
}

func objectIndices(obj *object.Object, parent bool) []bucketItem {
	as := obj.Attributes()

	res := make([]bucketItem, 0, 7+len(as)) // 7 predefined buckets and object attributes

	childfreeVal := v2object.BooleanPropertyValueTrue
	if len(obj.Children()) > 0 {
		childfreeVal = ""
	}

	res = append(res,
		bucketItem{
			key: v2object.FilterHeaderVersion,
			val: obj.Version().String(),
		},
		bucketItem{
			key: v2object.FilterHeaderContainerID,
			val: obj.ContainerID().String(),
		},
		bucketItem{
			key: v2object.FilterHeaderOwnerID,
			val: obj.OwnerID().String(),
		},
		bucketItem{
			key: v2object.FilterPropertyChildfree,
			val: childfreeVal,
		},
		bucketItem{
			key: v2object.FilterHeaderParent,
			val: obj.ParentID().String(),
		},
		bucketItem{
			key: v2object.FilterHeaderObjectID,
			val: obj.ID().String(),
		},
		// TODO: add remaining fields after neofs-api#72
	)

	if obj.Type() == objectSDK.TypeRegular && !obj.HasParent() {
		res = append(res, bucketItem{
			key: v2object.FilterPropertyRoot,
		})
	}

	if !parent {
		res = append(res, bucketItem{
			key: v2object.FilterPropertyPhy,
		})
	}

	for _, a := range as {
		res = append(res, bucketItem{
			key: a.Key(),
			val: a.Value(),
		})
	}

	return res
}
