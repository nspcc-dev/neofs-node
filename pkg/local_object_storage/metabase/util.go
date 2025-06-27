package meta

import (
	"crypto/sha256"
	"errors"
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

	// unusedPrimaryPrefix was deleted in metabase version 6
	unusedPrimaryPrefix
	// unusedLockersPrefix was deleted in metabase version 6
	unusedLockersPrefix
	// unusedStorageGroupPrefix was deleted in metabase version 6
	unusedStorageGroupPrefix
	// unusedTombstonePrefix was deleted in metabase version 6
	unusedTombstonePrefix
	// unusedSmallPrefix was deleted in metabase version 5
	unusedSmallPrefix
	// unusedRootPrefix was deleted in metabase version 5
	unusedRootPrefix

	//====================
	// FKBT index buckets.
	//====================

	// unusedOwnerPrefix was deleted in metabase version 5
	unusedOwnerPrefix
	// unusedUserAttributePrefix was deleted in metabase version 5
	unusedUserAttributePrefix

	//====================
	// List index buckets.
	//====================

	// unusedPayloadHashPrefix was deleted in metabase version 5
	unusedPayloadHashPrefix
	// unusedParentPrefix was deleted in metabase version 5
	unusedParentPrefix
	// unusedSplitPrefix was deleted in metabase version 5
	unusedSplitPrefix

	// garbageContainersPrefix is used for the garbage containers bucket.
	// 	Key: container ID
	// 	Value: dummy value
	garbageContainersPrefix

	// unusedLinkObjectsPrefix was deleted in metabase version 6
	unusedLinkObjectsPrefix

	// unusedFirstObjectIDPrefix was deleted in metabase version 5
	unusedFirstObjectIDPrefix
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
		return errors.New("invalid length")
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

// return true if provided object is of LOCK type.
func isLockObject(tx *bbolt.Tx, idCnr cid.ID, obj oid.ID) bool {
	var bkt = tx.Bucket(metaBucketKey(idCnr))
	if bkt == nil {
		return false
	}

	var typeKey = make([]byte, metaIDTypePrefixSize+len(object.TypeLock.String()))

	fillIDTypePrefix(typeKey)
	copy(typeKey[1:], obj[:])
	copy(typeKey[metaIDTypePrefixSize:], object.TypeLock.String())

	return bkt.Get(typeKey) != nil
}

func parseInt(s string) (*big.Int, bool) { return new(big.Int).SetString(s, 10) }

type keyBuffer []byte

func (x *keyBuffer) alloc(ln int) []byte {
	if len(*x) < ln {
		*x = make([]byte, ln)
	}
	return (*x)[:ln]
}
