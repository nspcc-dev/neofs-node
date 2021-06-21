package meta

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/nspcc-dev/neo-go/pkg/io"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
)

type (
	namedBucketItem struct {
		name, key, val []byte
	}
)

// PutPrm groups the parameters of Put operation.
type PutPrm struct {
	obj *object.Object

	id *blobovnicza.ID
}

// PutRes groups resulting values of Put operation.
type PutRes struct{}

// WithObject is a Put option to set object to save.
func (p *PutPrm) WithObject(obj *object.Object) *PutPrm {
	if p != nil {
		p.obj = obj
	}

	return p
}

// WithBlobovniczaID is a Put option to set blobovnicza ID to save.
func (p *PutPrm) WithBlobovniczaID(id *blobovnicza.ID) *PutPrm {
	if p != nil {
		p.id = id
	}

	return p
}

var (
	ErrUnknownObjectType        = errors.New("unknown object type")
	ErrIncorrectSplitInfoUpdate = errors.New("updating split info on object without it")
	ErrIncorrectRootObject      = errors.New("invalid root object")
)

// Put saves the object in DB.
func Put(db *DB, obj *object.Object, id *blobovnicza.ID) error {
	_, err := db.Put(new(PutPrm).
		WithObject(obj).
		WithBlobovniczaID(id),
	)

	return err
}

// Put saves object header in metabase. Object payload expected to be cut.
// Big objects have nil blobovniczaID.
func (db *DB) Put(prm *PutPrm) (res *PutRes, err error) {
	b := db.db.NewBatch()
	if err = db.put(b, prm.obj, prm.id, nil); err == nil {
		err = b.Commit(nil)
	}

	return
}

func (db *DB) put(tx *pebble.Batch, obj *object.Object, id *blobovnicza.ID, si *objectSDK.SplitInfo) error {
	isParent := si != nil

	exists, err := db.exists(obj.Address())

	if errors.As(err, &splitInfoError) {
		exists = true // object exists, however it is virtual
	} else if err != nil {
		return err // return any error besides SplitInfoError
	}

	// most right child and split header overlap parent so we have to
	// check if object exists to not overwrite it twice
	if exists {
		// when storage engine moves small objects from one blobovniczaID
		// to another, then it calls metabase.Put method with new blobovniczaID
		// and this code should be triggered
		if !isParent && id != nil {
			return updateBlobovniczaID(tx, obj.Address(), id)
		}

		// when storage already has last object in split hierarchy and there is
		// a linking object to put (or vice versa), we should update split info
		// with object ids of these objects
		if isParent {
			return db.updateSplitInfo(tx, obj.Address(), si)
		}

		return nil
	}

	if obj.GetParent() != nil && !isParent { // limit depth by two
		parentSI, err := splitInfoFromObject(obj)
		if err != nil {
			return err
		}

		err = db.put(tx, obj.GetParent(), id, parentSI)
		if err != nil {
			return err
		}
	}

	// build unique indexes
	uniqueIndexes, err := uniqueIndexes(obj, si, id)
	if err != nil {
		return fmt.Errorf("can' build unique indexes: %w", err)
	}

	// put unique indexes
	for i := range uniqueIndexes {
		err = putUniqueIndexItem(tx, uniqueIndexes[i])
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
		err = putListIndexItem(tx, listIndexes[i])
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
		err = putFKBTIndexItem(tx, fkbtIndexes[i])
		if err != nil {
			return err
		}
	}

	// update container volume size estimation
	if obj.Type() == objectSDK.TypeRegular && !isParent {
		err = db.changeContainerSize(
			tx,
			obj.ContainerID(),
			obj.PayloadSize(),
			true,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// builds list of <unique> indexes from the object.
func uniqueIndexes(obj *object.Object, si *objectSDK.SplitInfo, id *blobovnicza.ID) ([]namedBucketItem, error) {
	isParent := si != nil
	addr := obj.Address()
	objKey := objectKey(addr.ObjectID())
	result := make([]namedBucketItem, 0, 3)

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

		rawObject, err := object.NewRawFromObject(obj).CutPayload().Marshal()
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
				name: cidBucketKey(addr.ContainerID(), smallPrefix, nil),
				key:  objKey,
				val:  *id,
			})
		}
	}

	// index root object
	if obj.Type() == objectSDK.TypeRegular && !obj.HasParent() {
		var (
			err       error
			splitInfo []byte
		)

		if isParent {
			splitInfo, err = si.Marshal()
			if err != nil {
				return nil, fmt.Errorf("can't marshal split info: %w", err)
			}
		}

		result = append(result, namedBucketItem{
			name: cidBucketKey(addr.ContainerID(), rootPrefix, nil),
			key:  objKey,
			val:  splitInfo,
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
		name: cidBucketKey(addr.ContainerID(), payloadHashPrefix, nil),
		key:  obj.PayloadChecksum().Sum(),
		val:  objKey,
	})

	// index parent ids
	if obj.ParentID() != nil {
		result = append(result, namedBucketItem{
			name: cidBucketKey(addr.ContainerID(), parentPrefix, nil),
			key:  objectKey(obj.ParentID()),
			val:  objKey,
		})
	}

	// index split ids
	if obj.SplitID() != nil {
		result = append(result, namedBucketItem{
			name: cidBucketKey(addr.ContainerID(), splitPrefix, nil),
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
		name: cidBucketKey(addr.ContainerID(), ownerPrefix, nil),
		key:  []byte(obj.OwnerID().String()),
		val:  objKey,
	})

	// user specified attributes
	for i := range attrs {
		result = append(result, namedBucketItem{
			name: cidBucketKey(addr.ContainerID(), attributePrefix, []byte(attrs[i].Key())),
			key:  []byte(attrs[i].Value()),
			val:  objKey,
		})
	}

	return result, nil
}

func putUniqueIndexItem(tx *pebble.Batch, item namedBucketItem) error {
	key := appendKey(item.name, item.key)
	return tx.Set(key, item.val, nil)
}

func putFKBTIndexItem(tx *pebble.Batch, item namedBucketItem) error {
	key := appendKey(item.name, item.key)
	key = appendKey(key, item.val)
	return tx.Set(key, zeroValue, nil)
}

func putListIndexItem(tx *pebble.Batch, item namedBucketItem) error {
	return putFKBTIndexItem(tx, item)
}

// encodeList decodes list of bytes into a single blog for list bucket indexes.
func encodeList(lst [][]byte) ([]byte, error) {
	w := io.NewBufBinWriter()
	w.WriteVarUint(uint64(len(lst)))
	for i := range lst {
		w.WriteVarBytes(lst[i])
	}
	if w.Err != nil {
		return nil, w.Err
	}
	return w.Bytes(), nil
}

// decodeList decodes blob into the list of bytes from list bucket index.
func decodeList(data []byte) (lst [][]byte, err error) {
	if len(data) == 0 {
		return nil, nil
	}
	r := io.NewBinReaderFromBuf(data)
	l := r.ReadVarUint()
	lst = make([][]byte, l)
	for i := range lst {
		lst[i] = r.ReadVarBytes()
	}
	if r.Err != nil {
		return nil, r.Err
	}
	return lst, nil
}

// updateBlobovniczaID for existing objects if they were moved from from
// one blobovnicza to another.
func updateBlobovniczaID(tx *pebble.Batch, addr *objectSDK.Address, id *blobovnicza.ID) error {
	cidKey := cidBucketKey(addr.ContainerID(), smallPrefix, objectKey(addr.ObjectID()))
	return tx.Set(cidKey, *id, nil)
}

// splitInfoFromObject returns split info based on last or linkin object.
// Otherwise returns nil, nil.
func splitInfoFromObject(obj *object.Object) (*objectSDK.SplitInfo, error) {
	if obj.Parent() == nil {
		return nil, nil
	}

	info := objectSDK.NewSplitInfo()
	info.SetSplitID(obj.SplitID())

	switch {
	case isLinkObject(obj):
		info.SetLink(obj.ID())
	case isLastObject(obj):
		info.SetLastPart(obj.ID())
	default:
		return nil, ErrIncorrectRootObject // should never happen
	}

	return info, nil
}

// updateSpliInfo for existing objects if storage filled with extra information
// about last object in split hierarchy or linking object.
func (db *DB) updateSplitInfo(tx *pebble.Batch, addr *objectSDK.Address, from *objectSDK.SplitInfo) error {
	cidKey := cidBucketKey(addr.ContainerID(), rootPrefix, objectKey(addr.ObjectID()))
	rawSplitInfo, c, err := db.db.Get(cidKey)
	if err != nil {
		return ErrIncorrectSplitInfoUpdate
	}
	defer c.Close()

	to := objectSDK.NewSplitInfo()

	err = to.Unmarshal(rawSplitInfo)
	if err != nil {
		return fmt.Errorf("can't unmarshal split info from root index: %w", err)
	}

	result := util.MergeSplitInfo(from, to)

	rawSplitInfo, err = result.Marshal()
	if err != nil {
		return fmt.Errorf("can't marhsal merged split info: %w", err)
	}

	return tx.Set(cidKey, rawSplitInfo, nil)
}

// isLinkObject returns true if object contains parent header and list
// of children.
func isLinkObject(obj *object.Object) bool {
	return len(obj.Children()) > 0 && obj.Parent() != nil
}

// isLastObject returns true if object contains only parent header without list
// of children.
func isLastObject(obj *object.Object) bool {
	return len(obj.Children()) == 0 && obj.Parent() != nil
}
