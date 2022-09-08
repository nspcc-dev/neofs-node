package meta

import (
	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// StorageIDPrm groups the parameters of StorageID operation.
type StorageIDPrm struct {
	addr oid.Address
}

// StorageIDRes groups the resulting values of StorageID operation.
type StorageIDRes struct {
	id []byte
}

// SetAddress is a StorageID option to set the object address to check.
func (p *StorageIDPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// StorageID returns storage ID.
func (r StorageIDRes) StorageID() []byte {
	return r.id
}

// StorageID returns storage descriptor for objects from the blobstor.
// It is put together with the object can makes get/delete operation faster.
func (db *DB) StorageID(prm StorageIDPrm) (res StorageIDRes, err error) {
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res.id, err = db.storageID(tx, prm.addr)

		return err
	})

	return
}

func (db *DB) storageID(tx *bbolt.Tx, addr oid.Address) ([]byte, error) {
	key := make([]byte, bucketKeySize)
	smallBucket := tx.Bucket(smallBucketName(addr.Container(), key))
	if smallBucket == nil {
		return nil, nil
	}

	storageID := smallBucket.Get(objectKey(addr.Object(), key))
	if storageID == nil {
		return nil, nil
	}

	return slice.Copy(storageID), nil
}
