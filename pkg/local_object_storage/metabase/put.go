package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
	gio "io"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

type (
	namedBucketItem struct {
		name, key, val []byte
	}
)

// PutPrm groups the parameters of Put operation.
type PutPrm struct {
	obj *objectSDK.Object

	id *blobovnicza.ID
}

// PutRes groups the resulting values of Put operation.
type PutRes struct{}

// SetObject is a Put option to set object to save.
func (p *PutPrm) SetObject(obj *objectSDK.Object) {
	p.obj = obj
}

// SetBlobovniczaID is a Put option to set blobovnicza ID to save.
func (p *PutPrm) SetBlobovniczaID(id *blobovnicza.ID) {
	p.id = id
}

var (
	ErrUnknownObjectType        = errors.New("unknown object type")
	ErrIncorrectSplitInfoUpdate = errors.New("updating split info on object without it")
	ErrIncorrectRootObject      = errors.New("invalid root object")
)

// Put saves object header in metabase. Object payload expected to be cut.
// Big objects have nil blobovniczaID.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
func (db *DB) Put(prm PutPrm) (res PutRes, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	err = db.boltDB.Batch(func(tx *bbolt.Tx) error {
		return db.put(tx, prm.obj, prm.id, nil)
	})
	if err == nil {
		storagelog.Write(db.log,
			storagelog.AddressField(objectCore.AddressOf(prm.obj)),
			storagelog.OpField("metabase PUT"))
	}

	return
}

func (db *DB) put(tx *bbolt.Tx, obj *objectSDK.Object, id *blobovnicza.ID, si *objectSDK.SplitInfo) error {
	cnr, ok := obj.ContainerID()
	if !ok {
		return errors.New("missing container in object")
	}

	isParent := si != nil

	exists, err := db.exists(tx, object.AddressOf(obj))

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
			return updateBlobovniczaID(tx, object.AddressOf(obj), id)
		}

		// when storage already has last object in split hierarchy and there is
		// a linking object to put (or vice versa), we should update split info
		// with object ids of these objects
		if isParent {
			return updateSplitInfo(tx, object.AddressOf(obj), si)
		}

		return nil
	}

	if par := obj.Parent(); par != nil && !isParent { // limit depth by two
		parentSI, err := splitInfoFromObject(obj)
		if err != nil {
			return err
		}

		err = db.put(tx, par, id, parentSI)
		if err != nil {
			return err
		}
	}

	err = putUniqueIndexes(tx, obj, si, id)
	if err != nil {
		return fmt.Errorf("can't put unique indexes: %w", err)
	}

	err = updateListIndexes(tx, obj, putListIndexItem)
	if err != nil {
		return fmt.Errorf("can't put list indexes: %w", err)
	}

	err = updateFKBTIndexes(tx, obj, putFKBTIndexItem)
	if err != nil {
		return fmt.Errorf("can't put fake bucket tree indexes: %w", err)
	}

	// update container volume size estimation
	if obj.Type() == objectSDK.TypeRegular && !isParent {
		err = changeContainerSize(tx, cnr, obj.PayloadSize(), true)
		if err != nil {
			return err
		}
	}

	return nil
}

func putUniqueIndexes(
	tx *bbolt.Tx,
	obj *objectSDK.Object,
	si *objectSDK.SplitInfo,
	id *blobovnicza.ID,
) error {
	isParent := si != nil
	addr := object.AddressOf(obj)
	cnr := addr.Container()
	objKey := objectKey(addr.Object())

	// add value to primary unique bucket
	if !isParent {
		var bucketName []byte

		switch obj.Type() {
		case objectSDK.TypeRegular:
			bucketName = primaryBucketName(cnr)
		case objectSDK.TypeTombstone:
			bucketName = tombstoneBucketName(cnr)
		case objectSDK.TypeStorageGroup:
			bucketName = storageGroupBucketName(cnr)
		case objectSDK.TypeLock:
			bucketName = bucketNameLockers(cnr)
		default:
			return ErrUnknownObjectType
		}

		rawObject, err := obj.CutPayload().Marshal()
		if err != nil {
			return fmt.Errorf("can't marshal object header: %w", err)
		}

		err = putUniqueIndexItem(tx, namedBucketItem{
			name: bucketName,
			key:  objKey,
			val:  rawObject,
		})
		if err != nil {
			return err
		}

		// index blobovniczaID if it is present
		if id != nil {
			err = putUniqueIndexItem(tx, namedBucketItem{
				name: smallBucketName(cnr),
				key:  objKey,
				val:  *id,
			})
			if err != nil {
				return err
			}
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
				return fmt.Errorf("can't marshal split info: %w", err)
			}
		}

		err = putUniqueIndexItem(tx, namedBucketItem{
			name: rootBucketName(cnr),
			key:  objKey,
			val:  splitInfo,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

type updateIndexItemFunc = func(tx *bbolt.Tx, item namedBucketItem) error

func updateListIndexes(tx *bbolt.Tx, obj *objectSDK.Object, f updateIndexItemFunc) error {
	idObj, _ := obj.ID()
	cnr, _ := obj.ContainerID()
	objKey := objectKey(idObj)

	cs, _ := obj.PayloadChecksum()

	// index payload hashes
	err := f(tx, namedBucketItem{
		name: payloadHashBucketName(cnr),
		key:  cs.Value(),
		val:  objKey,
	})
	if err != nil {
		return err
	}

	idParent, ok := obj.ParentID()

	// index parent ids
	if ok {
		err := f(tx, namedBucketItem{
			name: parentBucketName(cnr),
			key:  objectKey(idParent),
			val:  objKey,
		})
		if err != nil {
			return err
		}
	}

	// index split ids
	if obj.SplitID() != nil {
		err := f(tx, namedBucketItem{
			name: splitBucketName(cnr),
			key:  obj.SplitID().ToV2(),
			val:  objKey,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func updateFKBTIndexes(tx *bbolt.Tx, obj *objectSDK.Object, f updateIndexItemFunc) error {
	id, _ := obj.ID()
	cnr, _ := obj.ContainerID()
	objKey := []byte(id.EncodeToString())

	attrs := obj.Attributes()

	err := f(tx, namedBucketItem{
		name: ownerBucketName(cnr),
		key:  []byte(obj.OwnerID().EncodeToString()),
		val:  objKey,
	})
	if err != nil {
		return err
	}

	// user specified attributes
	for i := range attrs {
		err := f(tx, namedBucketItem{
			name: attributeBucketName(cnr, attrs[i].Key()),
			key:  []byte(attrs[i].Value()),
			val:  objKey,
		})
		if err != nil {
			return err
		}
	}

	return nil
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

	var offset uint64
	size, n, err := getVarUint(data)
	if err != nil {
		return nil, err
	}

	offset += uint64(n)
	lst = make([][]byte, size, size+1)
	for i := range lst {
		sz, n, err := getVarUint(data[offset:])
		if err != nil {
			return nil, err
		}
		offset += uint64(n)

		next := offset + sz
		if uint64(len(data)) < next {
			return nil, gio.ErrUnexpectedEOF
		}
		lst[i] = data[offset:next]
		offset = next
	}
	return lst, nil
}

func getVarUint(data []byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, gio.ErrUnexpectedEOF
	}

	switch b := data[0]; b {
	case 0xfd:
		if len(data) < 3 {
			return 0, 1, gio.ErrUnexpectedEOF
		}
		return uint64(binary.LittleEndian.Uint16(data[1:])), 3, nil
	case 0xfe:
		if len(data) < 5 {
			return 0, 1, gio.ErrUnexpectedEOF
		}
		return uint64(binary.LittleEndian.Uint32(data[1:])), 5, nil
	case 0xff:
		if len(data) < 9 {
			return 0, 1, gio.ErrUnexpectedEOF
		}
		return binary.LittleEndian.Uint64(data[1:]), 9, nil
	default:
		return uint64(b), 1, nil
	}
}

// updateBlobovniczaID for existing objects if they were moved from from
// one blobovnicza to another.
func updateBlobovniczaID(tx *bbolt.Tx, addr oid.Address, id *blobovnicza.ID) error {
	bkt, err := tx.CreateBucketIfNotExists(smallBucketName(addr.Container()))
	if err != nil {
		return err
	}

	return bkt.Put(objectKey(addr.Object()), *id)
}

// updateSpliInfo for existing objects if storage filled with extra information
// about last object in split hierarchy or linking object.
func updateSplitInfo(tx *bbolt.Tx, addr oid.Address, from *objectSDK.SplitInfo) error {
	bkt := tx.Bucket(rootBucketName(addr.Container()))
	if bkt == nil {
		// if object doesn't exists and we want to update split info on it
		// then ignore, this should never happen
		return ErrIncorrectSplitInfoUpdate
	}

	objectKey := objectKey(addr.Object())

	rawSplitInfo := bkt.Get(objectKey)
	if len(rawSplitInfo) == 0 {
		return ErrIncorrectSplitInfoUpdate
	}

	to := objectSDK.NewSplitInfo()

	err := to.Unmarshal(rawSplitInfo)
	if err != nil {
		return fmt.Errorf("can't unmarshal split info from root index: %w", err)
	}

	result := util.MergeSplitInfo(from, to)

	rawSplitInfo, err = result.Marshal()
	if err != nil {
		return fmt.Errorf("can't marhsal merged split info: %w", err)
	}

	return bkt.Put(objectKey, rawSplitInfo)
}

// splitInfoFromObject returns split info based on last or linkin object.
// Otherwise returns nil, nil.
func splitInfoFromObject(obj *objectSDK.Object) (*objectSDK.SplitInfo, error) {
	if obj.Parent() == nil {
		return nil, nil
	}

	info := objectSDK.NewSplitInfo()
	info.SetSplitID(obj.SplitID())

	switch {
	case isLinkObject(obj):
		id, ok := obj.ID()
		if !ok {
			return nil, errors.New("missing object ID")
		}

		info.SetLink(id)
	case isLastObject(obj):
		id, ok := obj.ID()
		if !ok {
			return nil, errors.New("missing object ID")
		}

		info.SetLastPart(id)
	default:
		return nil, ErrIncorrectRootObject // should never happen
	}

	return info, nil
}

// isLinkObject returns true if object contains parent header and list
// of children.
func isLinkObject(obj *objectSDK.Object) bool {
	return len(obj.Children()) > 0 && obj.Parent() != nil
}

// isLastObject returns true if object contains only parent header without list
// of children.
func isLastObject(obj *objectSDK.Object) bool {
	return len(obj.Children()) == 0 && obj.Parent() != nil
}
