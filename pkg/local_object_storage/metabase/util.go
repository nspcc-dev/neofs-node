package meta

import (
	"strings"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
)

/*
We might increase performance by not using string representation of
identities and addresses. String representation require base58 encoding that
slows execution. Instead we can try to marshal these structures directly into
bytes. Check it later.
*/

var (
	graveyardBucketName = []byte("Graveyard")
	toMoveItBucketName  = []byte("ToMoveIt")

	zeroValue = []byte{0xFF}

	smallPostfix        = "_small"
	storageGroupPostfix = "_SG"
	tombstonePostfix    = "_TS"
	ownerPostfix        = "_ownerid"
	payloadHashPostfix  = "_payloadhash"
	rootPostfix         = "_root"
	parentPostfix       = "_parent"
	splitPostfix        = "_splitid"

	userAttributePostfix = "_attr_"

	splitInfoError *object.SplitInfoError // for errors.As comparisons
)

// primaryBucketName returns <CID>.
func primaryBucketName(cid *container.ID) []byte {
	return []byte(cid.String())
}

// tombstoneBucketName returns <CID>_TS.
func tombstoneBucketName(cid *container.ID) []byte {
	return []byte(cid.String() + tombstonePostfix)
}

// storageGroupBucketName returns <CID>_SG.
func storageGroupBucketName(cid *container.ID) []byte {
	return []byte(cid.String() + storageGroupPostfix)
}

// smallBucketName returns <CID>_small.
func smallBucketName(cid *container.ID) []byte {
	return []byte(cid.String() + smallPostfix) // consider caching output values
}

// attributeBucketName returns <CID>_attr_<attributeKey>.
func attributeBucketName(cid *container.ID, attributeKey string) []byte {
	sb := strings.Builder{} // consider getting string builders from sync.Pool
	sb.WriteString(cid.String())
	sb.WriteString(userAttributePostfix)
	sb.WriteString(attributeKey)

	return []byte(sb.String())
}

// payloadHashBucketName returns <CID>_payloadhash.
func payloadHashBucketName(cid *container.ID) []byte {
	return []byte(cid.String() + payloadHashPostfix)
}

// rootBucketName returns <CID>_root.
func rootBucketName(cid *container.ID) []byte {
	return []byte(cid.String() + rootPostfix)
}

// ownerBucketName returns <CID>_ownerid.
func ownerBucketName(cid *container.ID) []byte {
	return []byte(cid.String() + ownerPostfix)
}

// parentBucketName returns <CID>_parent.
func parentBucketName(cid *container.ID) []byte {
	return []byte(cid.String() + parentPostfix)
}

// splitBucketName returns <CID>_splitid.
func splitBucketName(cid *container.ID) []byte {
	return []byte(cid.String() + splitPostfix)
}

// addressKey returns key for K-V tables when key is a whole address.
func addressKey(addr *object.Address) []byte {
	return []byte(addr.String())
}

// objectKey returns key for K-V tables when key is an object id.
func objectKey(oid *object.ID) []byte {
	return []byte(oid.String())
}
