package meta

import (
	"bytes"
	"fmt"
	"strings"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

/*
We might increase performance by not using string representation of
identities and addresses. String representation require base58 encoding that
slows execution. Instead we can try to marshal these structures directly into
bytes. Check it later.
*/

const invalidBase58String = "_"

var (
	// graveyardBucketName stores rows with the objects that have been
	// covered with Tombstone objects. That objects should not be returned
	// from the node and should not be accepted by the node from other
	// nodes.
	graveyardBucketName = []byte(invalidBase58String + "Graveyard")
	// garbageBucketName stores rows with the objects that should be physically
	// deleted by the node (Garbage Collector routine).
	garbageBucketName         = []byte(invalidBase58String + "Garbage")
	toMoveItBucketName        = []byte(invalidBase58String + "ToMoveIt")
	containerVolumeBucketName = []byte(invalidBase58String + "ContainerSize")

	zeroValue = []byte{0xFF}

	smallPostfix        = invalidBase58String + "small"
	storageGroupPostfix = invalidBase58String + "SG"
	tombstonePostfix    = invalidBase58String + "TS"
	ownerPostfix        = invalidBase58String + "ownerid"
	payloadHashPostfix  = invalidBase58String + "payloadhash"
	rootPostfix         = invalidBase58String + "root"
	parentPostfix       = invalidBase58String + "parent"
	splitPostfix        = invalidBase58String + "splitid"

	userAttributePostfix = invalidBase58String + "attr_"

	splitInfoError *object.SplitInfoError // for errors.As comparisons
)

// primaryBucketName returns <CID>.
func primaryBucketName(cnr cid.ID) []byte {
	return []byte(cnr.EncodeToString())
}

// tombstoneBucketName returns <CID>_TS.
func tombstoneBucketName(cnr cid.ID) []byte {
	return []byte(cnr.EncodeToString() + tombstonePostfix)
}

// storageGroupBucketName returns <CID>_SG.
func storageGroupBucketName(cnr cid.ID) []byte {
	return []byte(cnr.EncodeToString() + storageGroupPostfix)
}

// smallBucketName returns <CID>_small.
func smallBucketName(cnr cid.ID) []byte {
	return []byte(cnr.EncodeToString() + smallPostfix) // consider caching output values
}

// attributeBucketName returns <CID>_attr_<attributeKey>.
func attributeBucketName(cnr cid.ID, attributeKey string) []byte {
	sb := strings.Builder{} // consider getting string builders from sync.Pool
	sb.WriteString(cnr.EncodeToString())
	sb.WriteString(userAttributePostfix)
	sb.WriteString(attributeKey)

	return []byte(sb.String())
}

// returns <CID> from attributeBucketName result, nil otherwise.
func cidFromAttributeBucket(val []byte, attributeKey string) []byte {
	suffix := []byte(userAttributePostfix + attributeKey)
	if !bytes.HasSuffix(val, suffix) {
		return nil
	}

	return val[:len(val)-len(suffix)]
}

// payloadHashBucketName returns <CID>_payloadhash.
func payloadHashBucketName(cnr cid.ID) []byte {
	return []byte(cnr.EncodeToString() + payloadHashPostfix)
}

// rootBucketName returns <CID>_root.
func rootBucketName(cnr cid.ID) []byte {
	return []byte(cnr.EncodeToString() + rootPostfix)
}

// ownerBucketName returns <CID>_ownerid.
func ownerBucketName(cnr cid.ID) []byte {
	return []byte(cnr.EncodeToString() + ownerPostfix)
}

// parentBucketName returns <CID>_parent.
func parentBucketName(cnr cid.ID) []byte {
	return []byte(cnr.EncodeToString() + parentPostfix)
}

// splitBucketName returns <CID>_splitid.
func splitBucketName(cnr cid.ID) []byte {
	return []byte(cnr.EncodeToString() + splitPostfix)
}

// addressKey returns key for K-V tables when key is a whole address.
func addressKey(addr oid.Address) []byte {
	return []byte(addr.EncodeToString())
}

// parses object address formed by addressKey.
func decodeAddressFromKey(dst *oid.Address, k []byte) error {
	err := dst.DecodeString(string(k))
	if err != nil {
		return fmt.Errorf("decode object address from db key: %w", err)
	}

	return nil
}

// objectKey returns key for K-V tables when key is an object id.
func objectKey(obj oid.ID) []byte {
	return []byte(obj.EncodeToString())
}

// removes all bucket elements.
func resetBucket(b *bbolt.Bucket) error {
	return b.ForEach(func(k, v []byte) error {
		if v != nil {
			return b.Delete(k)
		}

		return b.DeleteBucket(k)
	})
}

// if meets irregular object container in objs - returns its type, otherwise returns object.TypeRegular.
//
// firstIrregularObjectType(tx, cnr, obj) usage allows getting object type.
func firstIrregularObjectType(tx *bbolt.Tx, idCnr cid.ID, objs ...[]byte) object.Type {
	if len(objs) == 0 {
		panic("empty object list in firstIrregularObjectType")
	}

	irregularTypeBuckets := [...]struct {
		typ  object.Type
		name []byte
	}{
		{object.TypeTombstone, tombstoneBucketName(idCnr)},
		{object.TypeStorageGroup, storageGroupBucketName(idCnr)},
		{object.TypeLock, bucketNameLockers(idCnr)},
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
	return inBucket(tx, bucketNameLockers(idCnr), objectKey(obj))
}
