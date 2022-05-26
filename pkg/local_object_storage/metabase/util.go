package meta

import (
	"bytes"
	"strings"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
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
func primaryBucketName(cid *cid.ID) []byte {
	return []byte(cid.String())
}

// tombstoneBucketName returns <CID>_TS.
func tombstoneBucketName(cid *cid.ID) []byte {
	return []byte(cid.String() + tombstonePostfix)
}

// storageGroupBucketName returns <CID>_SG.
func storageGroupBucketName(cid *cid.ID) []byte {
	return []byte(cid.String() + storageGroupPostfix)
}

// smallBucketName returns <CID>_small.
func smallBucketName(cid *cid.ID) []byte {
	return []byte(cid.String() + smallPostfix) // consider caching output values
}

// attributeBucketName returns <CID>_attr_<attributeKey>.
func attributeBucketName(cid *cid.ID, attributeKey string) []byte {
	sb := strings.Builder{} // consider getting string builders from sync.Pool
	sb.WriteString(cid.String())
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
func payloadHashBucketName(cid *cid.ID) []byte {
	return []byte(cid.String() + payloadHashPostfix)
}

// rootBucketName returns <CID>_root.
func rootBucketName(cid *cid.ID) []byte {
	return []byte(cid.String() + rootPostfix)
}

// ownerBucketName returns <CID>_ownerid.
func ownerBucketName(cid *cid.ID) []byte {
	return []byte(cid.String() + ownerPostfix)
}

// parentBucketName returns <CID>_parent.
func parentBucketName(cid *cid.ID) []byte {
	return []byte(cid.String() + parentPostfix)
}

// splitBucketName returns <CID>_splitid.
func splitBucketName(cid *cid.ID) []byte {
	return []byte(cid.String() + splitPostfix)
}

// addressKey returns key for K-V tables when key is a whole address.
func addressKey(addr *addressSDK.Address) []byte {
	return []byte(addr.String())
}

// parses object address formed by addressKey.
func addressFromKey(k []byte) (*addressSDK.Address, error) {
	a := addressSDK.NewAddress()
	return a, a.Parse(string(k))
}

// objectKey returns key for K-V tables when key is an object id.
func objectKey(oid *oidSDK.ID) []byte {
	return []byte(oid.String())
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
		{object.TypeTombstone, tombstoneBucketName(&idCnr)},
		{object.TypeStorageGroup, storageGroupBucketName(&idCnr)},
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
func isLockObject(tx *bbolt.Tx, idCnr cid.ID, obj oidSDK.ID) bool {
	return inBucket(tx, bucketNameLockers(idCnr), objectKey(&obj))
}
