package meta

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/io"
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
	ErrUnknownObjectType          = errors.New("unknown object type")
	ErrIncorrectBlobovniczaUpdate = errors.New("updating blobovnicza id on object without it")
	ErrIncorrectSplitInfoUpdate   = errors.New("updating split info on object without it")
	ErrIncorrectRootObject        = errors.New("invalid root object")
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
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		return db.put(tx, prm.obj, prm.id, nil)
	})

	return
}

func (db *DB) put(tx *bbolt.Tx, obj *object.Object, id *blobovnicza.ID, si *objectSDK.SplitInfo) error {
	isParent := si != nil

	exists, err := db.exists(tx, obj.Address())

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
			return updateSplitInfo(tx, obj.Address(), si)
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
		err = changeContainerSize(
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
			bucketName = primaryBucketName(addr.ContainerID())
		case objectSDK.TypeTombstone:
			bucketName = tombstoneBucketName(addr.ContainerID())
		case objectSDK.TypeStorageGroup:
			bucketName = storageGroupBucketName(addr.ContainerID())
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
				name: smallBucketName(addr.ContainerID()),
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
			name: rootBucketName(addr.ContainerID()),
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

// updateSpliInfo for existing objects if storage filled with extra information
// about last object in split hierarchy or linking object.
func updateSplitInfo(tx *bbolt.Tx, addr *objectSDK.Address, from *objectSDK.SplitInfo) error {
	bkt := tx.Bucket(rootBucketName(addr.ContainerID()))
	if bkt == nil {
		// if object doesn't exists and we want to update split info on it
		// then ignore, this should never happen
		return ErrIncorrectSplitInfoUpdate
	}

	objectKey := objectKey(addr.ObjectID())

	rawSplitInfo := bkt.Get(objectKey)
	if len(rawSplitInfo) == 0 {
		return ErrIncorrectSplitInfoUpdate
	}

	to := objectSDK.NewSplitInfo()

	err := to.Unmarshal(rawSplitInfo)
	if err != nil {
		return fmt.Errorf("can't unmarshal split info from root index: %w", err)
	}

	result := mergeSplitInfo(from, to)

	rawSplitInfo, err = result.Marshal()
	if err != nil {
		return fmt.Errorf("can't marhsal merged split info: %w", err)
	}

	return bkt.Put(objectKey, rawSplitInfo)
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

// mergeSplitInfo ignores conflicts and rewrites `to` with non empty values
// from `from`.
func mergeSplitInfo(from, to *objectSDK.SplitInfo) *objectSDK.SplitInfo {
	to.SetSplitID(from.SplitID()) // overwrite SplitID and ignore conflicts

	if lp := from.LastPart(); lp != nil {
		to.SetLastPart(lp)
	}

	if link := from.Link(); link != nil {
		to.SetLink(link)
	}

	return to
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
