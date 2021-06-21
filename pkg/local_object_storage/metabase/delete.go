package meta

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
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

type referenceNumber struct {
	all, cur int

	addr *objectSDK.Address

	obj *object.Object
}

type referenceCounter map[string]*referenceNumber

// Delete removed object records from metabase indexes.
func (db *DB) Delete(prm *DeletePrm) (*DeleteRes, error) {
	tx := db.db.NewBatch()
	err := db.deleteGroup(tx, prm.addrs)
	if err == nil {
		err = tx.Commit(nil)
	}

	return nil, err
}

func (db *DB) deleteGroup(tx *pebble.Batch, addrs []*objectSDK.Address) error {
	refCounter := make(referenceCounter, len(addrs))

	for i := range addrs {
		err := db.delete(tx, addrs[i], refCounter)
		if err != nil {
			return err // maybe log and continue?
		}
	}

	for _, refNum := range refCounter {
		if refNum.cur == refNum.all {
			err := db.deleteObject(tx, refNum.obj, true)
			if err != nil {
				return err // maybe log and continue?
			}
		}
	}

	return nil
}

func (db *DB) delete(tx *pebble.Batch, addr *objectSDK.Address, refCounter referenceCounter) error {
	// remove record from graveyard
	key := append([]byte{graveyardPrefix}, addressKey(addr)...)
	err := tx.Delete(key, nil)
	if err != nil {
		return fmt.Errorf("could not remove from graveyard: %w", err)
	}

	// unmarshal object, work only with physically stored (raw == true) objects
	obj, err := db.get(addr, false, true)
	if err != nil {
		if errors.Is(err, object.ErrNotFound) {
			return nil
		}

		return err
	}

	// if object is an only link to a parent, then remove parent
	if parent := obj.GetParent(); parent != nil {
		parAddr := parent.Address()
		sParAddr := parAddr.String()

		nRef, ok := refCounter[sParAddr]
		if !ok {
			nRef = &referenceNumber{
				all:  db.parentLength(parent.Address()),
				addr: parAddr,
				obj:  parent,
			}

			refCounter[sParAddr] = nRef
		}

		nRef.cur++
	}

	// remove object
	return db.deleteObject(tx, obj, false)
}

func (db *DB) deleteObject(
	tx *pebble.Batch,
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
func (db *DB) parentLength(addr *objectSDK.Address) int {
	key := cidBucketKey(addr.ContainerID(), parentPrefix, objectKey(addr.ObjectID()))
	iter := db.newPrefixIterator(key)
	defer iter.Close()

	var cnt int
	for iter.First(); iter.Valid(); iter.Next() {
		cnt++
	}
	return cnt
}

func delUniqueIndexItem(tx *pebble.Batch, item namedBucketItem) {
	_ = tx.Delete(appendKey(item.name, item.key), nil) // ignore error, best effort there
}

func delFKBTIndexItem(tx *pebble.Batch, item namedBucketItem) {
	key := appendKey(item.name, item.key)
	key = appendKey(key, item.val)
	_ = tx.Delete(key, nil) // ignore error, best effort there
}

func delListIndexItem(tx *pebble.Batch, item namedBucketItem) {
	delFKBTIndexItem(tx, item)
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
			bucketName = cidBucketKey(addr.ContainerID(), primaryPrefix, nil)
		case objectSDK.TypeTombstone:
			bucketName = cidBucketKey(addr.ContainerID(), tombstonePrefix, nil)
		case objectSDK.TypeStorageGroup:
			bucketName = cidBucketKey(addr.ContainerID(), storageGroupPrefix, nil)
		default:
			return nil, ErrUnknownObjectType
		}

		result = append(result, namedBucketItem{
			name: bucketName,
			key:  objKey,
		})
	} else {
		name := cidBucketKey(obj.ContainerID(), parentPrefix, nil)
		result = append(result, namedBucketItem{
			name: name,
			key:  objKey,
		})
	}

	result = append(result,
		namedBucketItem{ // remove from small blobovnicza id index
			name: cidBucketKey(addr.ContainerID(), smallPrefix, nil),
			key:  objKey,
		},
		namedBucketItem{ // remove from root index
			name: cidBucketKey(addr.ContainerID(), rootPrefix, nil),
			key:  objKey,
		},
		namedBucketItem{ // remove from ToMoveIt index
			name: []byte{toMoveItPrefix},
			key:  addrKey,
		},
	)

	return result, nil
}
