package meta

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/big"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

const attributeDelimiterLen = 1

var (
	// graveyardBucketName stores rows with the objects that have been
	// covered with Tombstone objects. That objects should not be returned
	// from the node and should not be accepted by the node from other
	// nodes.
	graveyardBucketName = []byte{graveyardPrefix}
	// garbageObjectsBucketName stores rows with the objects that should be physically
	// deleted by the node (Garbage Collector routine).
	garbageObjectsBucketName = []byte{garbageObjectsPrefix}
	// garbageContainersBucketName stores rows with the containers that should be physically
	// deleted by the node (Garbage Collector routine).
	garbageContainersBucketName = []byte{garbageContainersPrefix}
	toMoveItBucketName          = []byte{toMoveItPrefix}
	containerVolumeBucketName   = []byte{containerVolumePrefix}

	zeroValue = []byte{0xFF}
)

// Prefix bytes for database keys. All ids and addresses are encoded in binary
// unless specified otherwise.
//
//nolint:godot
const (
	// graveyardPrefix is used for the graveyard bucket.
	// 	Key: object address
	// 	Value: tombstone address
	graveyardPrefix = iota
	// garbageObjectsPrefix is used for the garbage objects bucket.
	// 	Key: object address
	// 	Value: dummy value
	garbageObjectsPrefix
	// toMoveItPrefix is used for bucket containing IDs of objects that are candidates for moving
	// to another shard.
	toMoveItPrefix
	// containerVolumePrefix is used for storing container size estimations.
	//	Key: container ID
	//  Value: container size in bytes as little-endian uint64
	containerVolumePrefix
	// lockedPrefix is used for storing locked objects information.
	//  Key: container ID
	//  Value: bucket mapping objects locked to the list of corresponding LOCK objects.
	lockedPrefix
	// shardInfoPrefix is used for storing shard ID. All keys are custom and are not connected to the container.
	shardInfoPrefix

	//======================
	// Unique index buckets.
	//======================

	// primaryPrefix is used for prefixing buckets containing objects of REGULAR type.
	//  Key: object ID
	//  Value: marshalled object
	primaryPrefix
	// lockersPrefix is used for prefixing buckets containing objects of LOCK type.
	//  Key: object ID
	//  Value: marshalled object
	lockersPrefix
	// storageGroupPrefix is used for prefixing buckets containing objects of STORAGEGROUP type.
	//  Key: object ID
	//  Value: marshaled object
	storageGroupPrefix
	// tombstonePrefix is used for prefixing buckets containing objects of TOMBSTONE type.
	//  Key: object ID
	//  Value: marshaled object
	tombstonePrefix
	// smallPrefix is used for prefixing buckets mapping objects to the blobovniczas they are stored in.
	// Not really used now, blobovniczas are gone, but kept for compatibility.
	//  Key: object ID
	//  Value: blobovnicza ID
	smallPrefix
	// rootPrefix is used for prefixing buckets mapping parent object to the split info.
	//  Key: object ID
	//  Value: split info
	rootPrefix

	//====================
	// FKBT index buckets.
	//====================

	// ownerPrefix is used for prefixing FKBT index buckets mapping owner to object IDs.
	// Key: owner ID
	// Value: bucket containing object IDs as keys
	ownerPrefix
	// userAttributePrefix is used for prefixing FKBT index buckets containing objects.
	// Key: attribute value
	// Value: bucket containing object IDs as keys
	userAttributePrefix

	//====================
	// List index buckets.
	//====================

	// payloadHashPrefix is used for prefixing List index buckets mapping payload hash to a list of object IDs.
	//  Key: payload hash
	//  Value: list of object IDs
	payloadHashPrefix
	// parentPrefix is used for prefixing List index buckets mapping parent ID to a list of children IDs.
	//  Key: parent ID
	//  Value: list of object IDs
	parentPrefix
	// splitPrefix is used for prefixing List index buckets mapping split ID to a list of object IDs.
	//  Key: split ID
	//  Value: list of object IDs
	splitPrefix

	// garbageContainersPrefix is used for the garbage containers bucket.
	// 	Key: container ID
	// 	Value: dummy value
	garbageContainersPrefix

	// linkObjectsPrefix is used for prefixing buckets containing objects of LINK type.
	//  Key: object ID
	//  Value: marshaled object
	linkObjectsPrefix

	// firstObjectIDPrefix is used for prefixing List index buckets mapping first object ID to a list of objects IDs.
	//  Key: first object ID
	//  Value: list of object IDs
	firstObjectIDPrefix
)

// key prefix for per-container buckets storing objects' metadata required to
// serve ObjectService.SearchV2. See VERSION.md for details.
// This data can be completely migrated, so special byte is occupied.
const metadataPrefix = 255

const (
	cidSize        = sha256.Size
	bucketKeySize  = 1 + cidSize
	objectKeySize  = sha256.Size
	addressKeySize = cidSize + objectKeySize
)

var splitInfoError *object.SplitInfoError // for errors.As comparisons

func bucketName(cnr cid.ID, prefix byte, key []byte) []byte {
	key[0] = prefix
	copy(key[1:], cnr[:])
	return key[:bucketKeySize]
}

// primaryBucketName returns <CID>.
func primaryBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, primaryPrefix, key)
}

// tombstoneBucketName returns <CID>_TS.
func tombstoneBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, tombstonePrefix, key)
}

// storageGroupBucketName returns <CID>_SG.
func storageGroupBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, storageGroupPrefix, key)
}

// smallBucketName returns <CID>_small.
func smallBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, smallPrefix, key)
}

// linkObjectsBucketName returns link objects bucket key (`18<CID>`).
func linkObjectsBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, linkObjectsPrefix, key)
}

// attributeBucketName returns <CID>_attr_<attributeKey>.
func attributeBucketName(cnr cid.ID, attributeKey string, key []byte) []byte {
	key[0] = userAttributePrefix
	copy(key[1:], cnr[:])
	return append(key[:bucketKeySize], attributeKey...)
}

// returns <CID> from attributeBucketName result, nil otherwise.
func cidFromAttributeBucket(val []byte, attributeKey string) []byte {
	if len(val) < bucketKeySize || val[0] != userAttributePrefix || !bytes.Equal(val[bucketKeySize:], []byte(attributeKey)) {
		return nil
	}

	return val[1:bucketKeySize]
}

// payloadHashBucketName returns <CID>_payloadhash.
func payloadHashBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, payloadHashPrefix, key)
}

// rootBucketName returns <CID>_root.
func rootBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, rootPrefix, key)
}

// ownerBucketName returns <CID>_ownerid.
func ownerBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, ownerPrefix, key)
}

// parentBucketName returns <CID>_parent.
func parentBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, parentPrefix, key)
}

// splitBucketName returns <CID>_splitid.
func splitBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, splitPrefix, key)
}

func firstObjectIDBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, firstObjectIDPrefix, key)
}

// addressKey returns key for K-V tables when key is a whole address.
func addressKey(addr oid.Address, key []byte) []byte {
	cnr := addr.Container()
	obj := addr.Object()
	n := copy(key, cnr[:])
	copy(key[n:], obj[:])
	return key[:addressKeySize]
}

// parses object address formed by addressKey.
func decodeAddressFromKey(dst *oid.Address, k []byte) error {
	if len(k) != addressKeySize {
		return fmt.Errorf("invalid length")
	}

	var cnr cid.ID
	if err := cnr.Decode(k[:cidSize]); err != nil {
		return err
	}

	var obj oid.ID
	if err := obj.Decode(k[cidSize:]); err != nil {
		return err
	}

	dst.SetObject(obj)
	dst.SetContainer(cnr)
	return nil
}

// objectKey returns key for K-V tables when key is an object id.
func objectKey(obj oid.ID, key []byte) []byte {
	copy(key, obj[:])
	return key[:objectKeySize]
}

// containerKey returns key for K-V tables when key is a container ID.
func containerKey(cID cid.ID, key []byte) []byte {
	copy(key, cID[:])
	return key[:cidSize]
}

// if meets irregular object container in objs - returns its type, otherwise returns object.TypeRegular.
//
// firstIrregularObjectType(tx, cnr, obj) usage allows getting object type.
func firstIrregularObjectType(tx *bbolt.Tx, idCnr cid.ID, objs ...[]byte) object.Type {
	if len(objs) == 0 {
		panic("empty object list in firstIrregularObjectType")
	}

	var keys [4][1 + cidSize]byte

	irregularTypeBuckets := [...]struct {
		typ  object.Type
		name []byte
	}{
		{object.TypeTombstone, tombstoneBucketName(idCnr, keys[0][:])},
		{object.TypeStorageGroup, storageGroupBucketName(idCnr, keys[1][:])},
		{object.TypeLock, bucketNameLockers(idCnr, keys[2][:])},
		{object.TypeLink, linkObjectsBucketName(idCnr, keys[3][:])},
	}

	for i := range objs {
		for j := range irregularTypeBuckets {
			if inBucket(tx, irregularTypeBuckets[j].name, objs[i]) {
				return irregularTypeBuckets[j].typ
			}
		}
	}

	return object.TypeRegular
}

// return true if provided object is of LOCK type.
func isLockObject(tx *bbolt.Tx, idCnr cid.ID, obj oid.ID) bool {
	return inBucket(tx,
		bucketNameLockers(idCnr, make([]byte, bucketKeySize)),
		objectKey(obj, make([]byte, objectKeySize)))
}

func parseInt(s string) (*big.Int, bool) { return new(big.Int).SetString(s, 10) }

type keyBuffer []byte

func (x *keyBuffer) alloc(ln int) []byte {
	if len(*x) < ln {
		*x = make([]byte, ln)
	}
	return (*x)[:ln]
}
