package meta

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"go.etcd.io/bbolt"
)

type (
	namedBucketItem struct {
		name, key, val []byte
	}
)

var (
	ErrUnknownObjectType          = errors.New("unknown object type")
	ErrIncorrectBlobovniczaUpdate = errors.New("updating blobovnicza id on object without it")
)

// Put saves object header in metabase. Object payload expected to be cut.
// Big objects have nil blobovniczaID.
func (db *DB) Put(obj *object.Object, id *blobovnicza.ID) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		return db.put(tx, obj, id, false)
	})
}

func (db *DB) put(tx *bbolt.Tx, obj *object.Object, id *blobovnicza.ID, isParent bool) error {
	exists, err := db.exists(tx, obj.Address())
	if err != nil {
		return err
	}

	// most right child and split header overlap parent so we have to
	// check if object exists to not overwrite it twice
	if exists {
		// when storage engine moves small objects from one blobovniczaID
		// to another, then it calls metabase.Put method with new blobovniczaID
		// and this code should be triggered.
		if !isParent && id != nil {
			return updateBlobovniczaID(tx, obj.Address(), id)
		}

		return nil
	}

	if obj.GetParent() != nil && !isParent { // limit depth by two
		err = db.put(tx, obj.GetParent(), id, true)
		if err != nil {
			return err
		}
	}

	uniqueIndexes, err := uniqueIndexes(obj, isParent, id)
	if err != nil {
		return fmt.Errorf("can' build unique indexes: %w", err)
	}

	// put unique indexes
	for i := range uniqueIndexes {
		err := putUniqueIndexItem(tx, uniqueIndexes[i])
		if err != nil {
			return err
		}
	}

	// build list indexes
	listIndexes, err := listIndexes(obj)
	if err != nil {
		return fmt.Errorf("can' build list indexes: %w", err)
	}

	// put list indexes
	for i := range listIndexes {
		err := putListIndexItem(tx, listIndexes[i])
		if err != nil {
			return err
		}
	}

	// build fake bucket tree indexes
	fkbtIndexes, err := fkbtIndexes(obj)
	if err != nil {
		return fmt.Errorf("can' build fake bucket tree indexes: %w", err)
	}

	// put fake bucket tree indexes
	for i := range fkbtIndexes {
		err := putFKBTIndexItem(tx, fkbtIndexes[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// builds list of <unique> indexes from the object.
func uniqueIndexes(obj *object.Object, isParent bool, id *blobovnicza.ID) ([]namedBucketItem, error) {
	addr := obj.Address()
	objKey := objectKey(addr.ObjectID())
	result := make([]namedBucketItem, 0, 3)

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

		rawObject, err := obj.Marshal()
		if err != nil {
			return nil, fmt.Errorf("can't marshal object header: %w", err)
		}

		result = append(result, namedBucketItem{
			name: bucketName,
			key:  objKey,
			val:  rawObject,
		})

		// index blobovniczaID if it is present
		if id != nil {
			result = append(result, namedBucketItem{
				name: smallBucketName(addr.ContainerID()),
				key:  objKey,
				val:  *id,
			})
		}
	}

	// index root object
	if obj.Type() == objectSDK.TypeRegular && !obj.HasParent() {
		result = append(result, namedBucketItem{
			name: rootBucketName(addr.ContainerID()),
			key:  objKey,
			val:  zeroValue, // todo: store split.Info when it will be ready
		})
	}

	return result, nil
}

// builds list of <list> indexes from the object.
func listIndexes(obj *object.Object) ([]namedBucketItem, error) {
	result := make([]namedBucketItem, 0, 3)
	addr := obj.Address()
	objKey := objectKey(addr.ObjectID())

	// index payload hashes
	result = append(result, namedBucketItem{
		name: payloadHashBucketName(addr.ContainerID()),
		key:  obj.PayloadChecksum().Sum(),
		val:  objKey,
	})

	// index parent ids
	if obj.ParentID() != nil {
		result = append(result, namedBucketItem{
			name: parentBucketName(addr.ContainerID()),
			key:  objectKey(obj.ParentID()),
			val:  objKey,
		})
	}

	// index split ids
	if obj.SplitID() != nil {
		result = append(result, namedBucketItem{
			name: splitBucketName(addr.ContainerID()),
			key:  obj.SplitID().ToV2(),
			val:  objKey,
		})
	}

	return result, nil
}

// builds list of <fake bucket tree> indexes from the object.
func fkbtIndexes(obj *object.Object) ([]namedBucketItem, error) {
	addr := obj.Address()
	objKey := []byte(addr.ObjectID().String())

	attrs := obj.Attributes()
	result := make([]namedBucketItem, 0, 1+len(attrs))

	// owner
	result = append(result, namedBucketItem{
		name: ownerBucketName(addr.ContainerID()),
		key:  []byte(obj.OwnerID().String()),
		val:  objKey,
	})

	// user specified attributes
	for i := range attrs {
		result = append(result, namedBucketItem{
			name: attributeBucketName(addr.ContainerID(), attrs[i].Key()),
			key:  []byte(attrs[i].Value()),
			val:  objKey,
		})
	}

	return result, nil
}

func putUniqueIndexItem(tx *bbolt.Tx, item namedBucketItem) error {
	bkt, err := tx.CreateBucketIfNotExists(item.name)
	if err != nil {
		return fmt.Errorf("can't create index %v: %w", item.name, err)
	}

	return bkt.Put(item.key, item.val)
}

func putFKBTIndexItem(tx *bbolt.Tx, item namedBucketItem) error {
	bkt, err := tx.CreateBucketIfNotExists(item.name)
	if err != nil {
		return fmt.Errorf("can't create index %v: %w", item.name, err)
	}

	fkbtRoot, err := bkt.CreateBucketIfNotExists(item.key)
	if err != nil {
		return fmt.Errorf("can't create fake bucket tree index %v: %w", item.key, err)
	}

	return fkbtRoot.Put(item.val, zeroValue)
}

func putListIndexItem(tx *bbolt.Tx, item namedBucketItem) error {
	bkt, err := tx.CreateBucketIfNotExists(item.name)
	if err != nil {
		return fmt.Errorf("can't create index %v: %w", item.name, err)
	}

	lst, err := decodeList(bkt.Get(item.key))
	if err != nil {
		return fmt.Errorf("can't decode leaf list %v: %w", item.key, err)
	}

	lst = append(lst, item.val)

	encodedLst, err := encodeList(lst)
	if err != nil {
		return fmt.Errorf("can't encode leaf list %v: %w", item.key, err)
	}

	return bkt.Put(item.key, encodedLst)
}

// encodeList decodes list of bytes into a single blog for list bucket indexes.
func encodeList(lst [][]byte) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buf)

	// consider using protobuf encoding instead of glob
	if err := encoder.Encode(lst); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// decodeList decodes blob into the list of bytes from list bucket index.
func decodeList(data []byte) (lst [][]byte, err error) {
	if len(data) == 0 {
		return nil, nil
	}

	decoder := gob.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&lst); err != nil {
		return nil, err
	}

	return lst, nil
}

// updateBlobovniczaID for existing objects if they were moved from from
// one blobovnicza to another.
func updateBlobovniczaID(tx *bbolt.Tx, addr *objectSDK.Address, id *blobovnicza.ID) error {
	bkt := tx.Bucket(smallBucketName(addr.ContainerID()))
	if bkt == nil {
		// if object exists, don't have blobovniczaID and we want to update it
		// then ignore, this should never happen
		return ErrIncorrectBlobovniczaUpdate
	}

	objectKey := objectKey(addr.ObjectID())

	if len(bkt.Get(objectKey)) == 0 {
		return ErrIncorrectBlobovniczaUpdate
	}

	return bkt.Put(objectKey, *id)
}
