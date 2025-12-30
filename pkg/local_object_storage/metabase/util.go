package meta

import (
	"bytes"
	"crypto/sha256"
	"math/big"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

const attributeDelimiterLen = 1

var (
	containerVolumeBucketName = []byte{containerVolumePrefix}

	// containerGCMarkKey marker key inside meta bucket designating whole container GC-marked.
	containerGCMarkKey = []byte{metaPrefixGC}

	zeroValue = []byte{0xFF}
)

// Prefix bytes for database keys. All ids and addresses are encoded in binary
// unless specified otherwise.
//
//nolint:godot
const (
	// unusedGraveyardPrefix was deleted in metabase version 9
	unusedGraveyardPrefix = iota
	// unusedGarbageObjectsPrefix was deleted in metabase version 9
	unusedGarbageObjectsPrefix
	// unusedToMoveItPrefix was deleted in metabase version 9
	unusedToMoveItPrefix
	// containerVolumePrefix is used for storing container size estimations.
	//	Key: container ID
	//  Value: container size in bytes as little-endian uint64
	containerVolumePrefix
	// unusedLockedPrefix was deleted in metabase version 9
	unusedLockedPrefix
	// shardInfoPrefix is used for storing shard ID. All keys are custom and are not connected to the container.
	shardInfoPrefix

	// ======================
	// Unique index buckets.
	// ======================

	// unusedPrimaryPrefix was deleted in metabase version 6
	unusedPrimaryPrefix
	// unusedLockersPrefix was deleted in metabase version 6
	unusedLockersPrefix
	// unusedStorageGroupPrefix was deleted in metabase version 6
	unusedStorageGroupPrefix
	// unusedTombstonePrefix was deleted in metabase version 6
	unusedTombstonePrefix
	// unusedSmallPrefix was deleted in metabase version 5
	unusedSmallPrefix //nolint:unused
	// unusedRootPrefix was deleted in metabase version 5
	unusedRootPrefix //nolint:unused

	// ====================
	// FKBT index buckets.
	// ====================

	// unusedOwnerPrefix was deleted in metabase version 5
	unusedOwnerPrefix //nolint:unused
	// unusedUserAttributePrefix was deleted in metabase version 5
	unusedUserAttributePrefix //nolint:unused

	// ====================
	// List index buckets.
	// ====================

	// unusedPayloadHashPrefix was deleted in metabase version 5
	unusedPayloadHashPrefix //nolint:unused
	// unusedParentPrefix was deleted in metabase version 5
	unusedParentPrefix //nolint:unused
	// unusedSplitPrefix was deleted in metabase version 5
	unusedSplitPrefix //nolint:unused

	// unusedGarbageContainersPrefix was deleted in metabase version 8
	unusedGarbageContainersPrefix

	// unusedLinkObjectsPrefix was deleted in metabase version 6
	unusedLinkObjectsPrefix

	// unusedFirstObjectIDPrefix was deleted in metabase version 5
	unusedFirstObjectIDPrefix //nolint:unused
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

// return true if provided object is of LOCK type.
func isLockObject(cur *bbolt.Cursor, obj oid.ID) bool {
	var typeKey = make([]byte, metaIDTypePrefixSize+len(object.TypeLock.String()))

	fillIDTypePrefix(typeKey)
	copy(typeKey[1:], obj[:])
	copy(typeKey[metaIDTypePrefixSize:], object.TypeLock.String())

	k, _ := cur.Seek(typeKey)

	return bytes.Equal(k, typeKey)
}

func parseInt(s string) (*big.Int, bool) { return new(big.Int).SetString(s, 10) }

type keyBuffer []byte

func (x *keyBuffer) alloc(ln int) []byte {
	if len(*x) < ln {
		*x = make([]byte, ln)
	}
	return (*x)[:ln]
}

// containerMarkedGC checks if the container is marked by GC.
// metaCursor must be not nil.
func containerMarkedGC(metaCursor *bbolt.Cursor) bool {
	k, _ := metaCursor.Seek(containerGCMarkKey)
	return bytes.Equal(k, containerGCMarkKey)
}
