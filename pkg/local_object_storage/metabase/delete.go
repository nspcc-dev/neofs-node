package meta

import (
	"bytes"
	"fmt"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"go.etcd.io/bbolt"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addrs []*objectSDK.Address
}

// DeleteRes groups resulting values of Delete operation.
type DeleteRes struct{}

// WithAddresses is a Delete option to set the addresses of the objects to delete.
//
// Option is required.
func (p *DeletePrm) WithAddresses(addrs ...*objectSDK.Address) *DeletePrm {
	if p != nil {
		p.addrs = addrs
	}

	return p
}

// Delete removes objects from DB.
func Delete(db *DB, addrs ...*objectSDK.Address) error {
	_, err := db.Delete(new(DeletePrm).WithAddresses(addrs...))
	return err
}

// Delete removed object records from metabase indexes.
func (db *DB) Delete(prm *DeletePrm) (*DeleteRes, error) {
	return new(DeleteRes), db.boltDB.Update(func(tx *bbolt.Tx) error {
		for i := range prm.addrs {
			err := db.delete(tx, prm.addrs[i], false)
			if err != nil {
				return err // maybe log and continue?
			}
		}

		return nil
	})
}

func (db *DB) delete(tx *bbolt.Tx, addr *objectSDK.Address, isParent bool) error {
	// unmarshal object, work only with physically stored (raw == true) objects
	obj, err := db.get(tx, addr, false, true)
	if err != nil {
		return err
	}

	// if object is an only link to a parent, then remove parent
	if parent := obj.GetParent(); parent != nil {
		if parentLength(tx, parent.Address()) == 1 {
			err = db.deleteObject(tx, obj.GetParent(), true)
			if err != nil {
				return err
			}
		}
	}

	// remove object
	return db.deleteObject(tx, obj, isParent)
}

func (db *DB) deleteObject(
	tx *bbolt.Tx,
	obj *object.Object,
	isParent bool,
) error {
	uniqueIndexes, err := delUniqueIndexes(obj, isParent)
	if err != nil {
		return fmt.Errorf("can' build unique indexes: %w", err)
	}

	// delete unique indexes
	for i := range uniqueIndexes {
		delUniqueIndexItem(tx, uniqueIndexes[i])
	}

	// build list indexes
	listIndexes, err := listIndexes(obj)
	if err != nil {
		return fmt.Errorf("can' build list indexes: %w", err)
	}

	// delete list indexes
	for i := range listIndexes {
		delListIndexItem(tx, listIndexes[i])
	}

	// build fake bucket tree indexes
	fkbtIndexes, err := fkbtIndexes(obj)
	if err != nil {
		return fmt.Errorf("can' build fake bucket tree indexes: %w", err)
	}

	// delete fkbt indexes
	for i := range fkbtIndexes {
		delFKBTIndexItem(tx, fkbtIndexes[i])
	}

	return nil
}

// parentLength returns amount of available children from parentid index.
func parentLength(tx *bbolt.Tx, addr *objectSDK.Address) int {
	bkt := tx.Bucket(parentBucketName(addr.ContainerID()))
	if bkt == nil {
		return 0
	}

	lst, err := decodeList(bkt.Get(objectKey(addr.ObjectID())))
	if err != nil {
		return 0
	}

	return len(lst)
}

func delUniqueIndexItem(tx *bbolt.Tx, item namedBucketItem) {
	bkt := tx.Bucket(item.name)
	if bkt != nil {
		_ = bkt.Delete(item.key) // ignore error, best effort there
	}
}

func delFKBTIndexItem(tx *bbolt.Tx, item namedBucketItem) {
	bkt := tx.Bucket(item.name)
	if bkt == nil {
		return
	}

	fkbtRoot := bkt.Bucket(item.key)
	if fkbtRoot == nil {
		return
	}

	_ = fkbtRoot.Delete(item.val) // ignore error, best effort there
}

func delListIndexItem(tx *bbolt.Tx, item namedBucketItem) {
	bkt := tx.Bucket(item.name)
	if bkt == nil {
		return
	}

	lst, err := decodeList(bkt.Get(item.key))
	if err != nil || len(lst) == 0 {
		return
	}

	// remove element from the list
	newLst := make([][]byte, 0, len(lst))

	for i := range lst {
		if !bytes.Equal(item.val, lst[i]) {
			newLst = append(newLst, lst[i])
		}
	}

	// if list empty, remove the key from <list> bucket
	if len(newLst) == 0 {
		_ = bkt.Delete(item.key) // ignore error, best effort there

		return
	}

	// if list is not empty, then update it
	encodedLst, err := encodeList(lst)
	if err != nil {
		return // ignore error, best effort there
	}

	_ = bkt.Put(item.key, encodedLst) // ignore error, best effort there
}

func delUniqueIndexes(obj *object.Object, isParent bool) ([]namedBucketItem, error) {
	addr := obj.Address()
	objKey := objectKey(addr.ObjectID())
	addrKey := addressKey(addr)

	result := make([]namedBucketItem, 0, 5)

	// add value to primary unique bucket
	if !isParent {
		var bucketName []byte

		switch obj.Type() {
		case objectSDK.TypeRegular:
			bucketName = primaryBucketName(addr.ContainerID())
		case objectSDK.TypeTombstone:
			bucketName = tombstoneBucketName(addr.ContainerID())
		case objectSDK.TypeStorageGroup:
			bucketName = storageGroupBucketName(addr.ContainerID())
		default:
			return nil, ErrUnknownObjectType
		}

		result = append(result, namedBucketItem{
			name: bucketName,
			key:  objKey,
		})
	}

	result = append(result,
		namedBucketItem{ // remove from small blobovnicza id index
			name: smallBucketName(addr.ContainerID()),
			key:  objKey,
		},
		namedBucketItem{ // remove from root index
			name: rootBucketName(addr.ContainerID()),
			key:  objKey,
		},
		namedBucketItem{ // remove from graveyard index
			name: graveyardBucketName,
			key:  addrKey,
		},
		namedBucketItem{ // remove from ToMoveIt index
			name: toMoveItBucketName,
			key:  addrKey,
		},
	)

	return result, nil
}
