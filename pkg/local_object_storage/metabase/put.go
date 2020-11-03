package meta

import (
	"bytes"
	"encoding/gob"

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
			indices := objectIndices(obj, par)

			for i := range indices {
				// create index bucket
				keyBucket, err := indexBucket.CreateBucketIfNotExists([]byte(indices[i].key))
				if err != nil {
					return errors.Wrapf(err, "(%T) could not create bucket for header key", db)
				}

				v := nonEmptyKeyBytes([]byte(indices[i].val))

				strs, err := decodeAddressList(keyBucket.Get(v))
				if err != nil {
					return errors.Wrapf(err, "(%T) could not decode address list", db)
				}

				data, err := encodeAddressList(append(strs, string(addrKey)))
				if err != nil {
					return errors.Wrapf(err, "(%T) could not encode address list", db)
				}

				if err := keyBucket.Put(v, data); err != nil {
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
	as := obj.GetAttributes()

	res := make([]bucketItem, 0, 5+len(as))

	rootVal := v2object.BooleanPropertyValueTrue
	if obj.HasParent() {
		rootVal = ""
	}

	leafVal := v2object.BooleanPropertyValueTrue
	if parent {
		leafVal = ""
	}

	childfreeVal := v2object.BooleanPropertyValueTrue
	if len(obj.GetChildren()) > 0 {
		childfreeVal = ""
	}

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
		bucketItem{
			key: v2object.FilterPropertyRoot,
			val: rootVal,
		},
		bucketItem{
			key: v2object.FilterPropertyLeaf,
			val: leafVal,
		},
		bucketItem{
			key: v2object.FilterPropertyChildfree,
			val: childfreeVal,
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

// FIXME: gob is a temporary solution, use protobuf.
func decodeAddressList(data []byte) ([]string, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var strs []string

	decoder := gob.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&strs); err != nil {
		return nil, err
	}

	return strs, nil
}

func encodeAddressList(l []string) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buf)

	if err := encoder.Encode(l); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
