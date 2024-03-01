package meta

import (
	"bytes"
	"errors"

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
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return res, ErrDegradedMode
	}

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

	return bytes.Clone(storageID), nil
}

// UpdateStorageIDPrm groups the parameters of UpdateStorageID operation.
type UpdateStorageIDPrm struct {
	addr oid.Address
	id   []byte
}

// UpdateStorageIDRes groups the resulting values of UpdateStorageID operation.
type UpdateStorageIDRes struct{}

// SetAddress is an UpdateStorageID option to set the object address to check.
func (p *UpdateStorageIDPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// SetStorageID is an UpdateStorageID option to set the storage ID.
func (p *UpdateStorageIDPrm) SetStorageID(id []byte) {
	p.id = id
}

// UpdateStorageID updates storage descriptor for objects from the blobstor.
func (db *DB) UpdateStorageID(prm UpdateStorageIDPrm) (res UpdateStorageIDRes, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return res, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return res, ErrReadOnlyMode
	}

	currEpoch := db.epochState.CurrentEpoch()

	err = db.boltDB.Batch(func(tx *bbolt.Tx) error {
		exists, err := db.exists(tx, prm.addr, currEpoch)
		if err == nil && exists || errors.Is(err, ErrObjectIsExpired) {
			err = updateStorageID(tx, prm.addr, prm.id)
		}

		return err
	})

	return
}
