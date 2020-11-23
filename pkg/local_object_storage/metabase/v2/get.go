package meta

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"go.etcd.io/bbolt"
)

// Get returns object header for specified address.
func (db *DB) Get(addr *objectSDK.Address) (obj *object.Object, err error) {
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		obj, err = db.get(tx, addr)

		return err
	})

	return obj, err
}

func (db *DB) get(tx *bbolt.Tx, addr *objectSDK.Address) (obj *object.Object, err error) {
	obj = object.New()
	key := objectKey(addr.ObjectID())
	cid := addr.ContainerID()

	if inGraveyard(tx, addr) {
		return nil, ErrAlreadyRemoved
	}

	// check in primary index
	data := getFromBucket(tx, primaryBucketName(cid), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in tombstone index
	data = getFromBucket(tx, tombstoneBucketName(cid), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in storage group index
	data = getFromBucket(tx, storageGroupBucketName(cid), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	return nil, ErrNotFound
}

func getFromBucket(tx *bbolt.Tx, name, key []byte) []byte {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return nil
	}

	return bkt.Get(key)
}
