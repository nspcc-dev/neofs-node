package meta

import (
	"fmt"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// ObjectStatus represents the status of the object in the Metabase.
type ObjectStatus struct {
	State     []string
	Path      string
	StorageID string
	Error     error
}

// ObjectStatus returns the status of the object in the Metabase. It contains state, path and storageID.
func (db *DB) ObjectStatus(address oid.Address) (ObjectStatus, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()
	var res ObjectStatus
	if db.mode.NoMetabase() {
		return res, nil
	}

	storageID := StorageIDPrm{}
	storageID.SetAddress(address)
	resStorageID, err := db.StorageID(storageID)
	if err != nil {
		res.Error = fmt.Errorf("reading storage ID: %w", err)
		return res, res.Error
	}

	if id := resStorageID.StorageID(); id != nil {
		res.StorageID = string(id)
	}

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		oID := address.Object()
		cID := address.Container()
		objKey := objectKey(address.Object(), make([]byte, objectKeySize))
		key := make([]byte, bucketKeySize)

		if objectLocked(tx, cID, oID) {
			res.State = append(res.State, "LOCKED")
		}

		graveyardBkt := tx.Bucket(graveyardBucketName)
		garbageObjectsBkt := tx.Bucket(garbageObjectsBucketName)
		garbageContainersBkt := tx.Bucket(garbageContainersBucketName)
		addrKey := addressKey(address, make([]byte, addressKeySize))

		removedStatus := inGraveyardWithKey(addrKey, graveyardBkt, garbageObjectsBkt, garbageContainersBkt)

		if removedStatus != 0 && objectLocked(tx, cID, oID) || inBucket(tx, primaryBucketName(cID, key), objKey) || inBucket(tx, parentBucketName(cID, key), objKey) {
			res.State = append(res.State, "AVAILABLE")
		}
		if removedStatus == 1 {
			res.State = append(res.State, "GC MARKED")
		}
		if removedStatus == 2 {
			res.State = append(res.State, "IN GRAVEYARD")
		}
		return err
	})
	res.Path = db.boltDB.Path()
	res.Error = err
	return res, err
}
