package meta

import (
	"fmt"
	"testing"

	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestCorruptedObject(t *testing.T) {
	db := newDB(t)

	o := objecttest.Object()
	o.ResetRelations()
	o.SetType(objectSDK.TypeRegular)
	cID := o.GetContainerID()
	oID := o.GetID()
	addr := oid.NewAddress(cID, oID)

	err := db.Put(&o, nil)
	require.NoError(t, err)

	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(primaryBucketName(cID, make([]byte, bucketKeySize)))
		if bkt == nil {
			return nil
		}

		// corrupt object
		err = bkt.Put(objectKey(oID, make([]byte, objectKeySize)), make([]byte, 1<<10))
		if err != nil {
			return fmt.Errorf("currupt object data: %w", err)
		}

		return nil
	})
	require.NoError(t, err)

	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		_, _, _, err = db.delete(tx, addr, 0)
		if err != nil {
			return fmt.Errorf("delete object: %w", err)
		}

		return nil
	})
	require.NoError(t, err)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		bktsWithLists := []byte{
			primaryPrefix,
			tombstonePrefix,
			storageGroupPrefix,
			lockersPrefix,
			linkObjectsPrefix,
			rootPrefix,
		}

		for _, prefix := range bktsWithLists {
			bKey := make([]byte, bucketKeySize)
			bKey[0] = prefix
			copy(bKey[1:], cID[:])

			b := tx.Bucket(bKey)
			if b != nil {
				err = b.ForEach(func(k, v []byte) error {
					return fmt.Errorf("%d-prefix bucket was not cleaned", prefix)
				})
				if err != nil {
					return err
				}
			}
		}

		b := tx.Bucket([]byte{toMoveItPrefix})
		err = b.ForEach(func(k, v []byte) error {
			return fmt.Errorf("%d-prefix bucket was not cleaned", toMoveItPrefix)
		})
		if err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)
}
