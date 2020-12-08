package meta

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"go.etcd.io/bbolt"
)

// IsSmall returns blobovniczaID for small objects and nil for big objects.
// Small objects stored in blobovnicza instances. Big objects stored in FS by
// shallow path which is calculated from address and therefore it is not
// indexed in metabase.
func (db *DB) IsSmall(addr *objectSDK.Address) (id *blobovnicza.ID, err error) {
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		id, err = db.isSmall(tx, addr)

		return err
	})

	return id, err
}

func (db *DB) isSmall(tx *bbolt.Tx, addr *objectSDK.Address) (*blobovnicza.ID, error) {
	smallBucket := tx.Bucket(smallBucketName(addr.ContainerID()))
	if smallBucket == nil {
		return nil, nil
	}

	blobovniczaID := smallBucket.Get(objectKey(addr.ObjectID()))
	if len(blobovniczaID) == 0 {
		return nil, nil
	}

	return blobovnicza.NewIDFromBytes(blobovniczaID), nil
}
