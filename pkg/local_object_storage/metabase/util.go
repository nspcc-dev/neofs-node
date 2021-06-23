package meta

import (
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
)

/*
We might increase performance by not using string representation of
identities and addresses. String representation require base58 encoding that
slows execution. Instead we can try to marshal these structures directly into
bytes. Check it later.
*/

const (
	graveyardPrefix       = 0x01
	toMoveItPrefix        = 0x02
	containerVolumePrefix = 0x03
	primaryPrefix         = 0x04
	smallPrefix           = 0x05
	tombstonePrefix       = 0x06
	storageGroupPrefix    = 0x07
	ownerPrefix           = 0x08
	payloadHashPrefix     = 0x09
	rootPrefix            = 0x0A
	parentPrefix          = 0x0B
	splitPrefix           = 0x0C
	attributePrefix       = 0x0D
)

var (
	zeroValue = []byte{0xFF}

	splitInfoError *object.SplitInfoError // for errors.As comparisons
)

func cidBucketKey(cid *cid.ID, bucket byte, subKey []byte) []byte {
	cidBytes := cid.ToV2().GetValue()
	key := make([]byte, 0, 1+1+len(cidBytes))
	key = append(key, bucket)
	key = appendKey(key, cidBytes)
	if subKey != nil {
		key = appendKey(key, subKey)
	}

	return key
}

func splitKey(key []byte) [][]byte {
	var res [][]byte
	if len(key) <= 1 {
		return res
	}

	for i := 1; i < len(key); {
		sz := int(key[i])
		res = append(res, key[i+1:i+1+sz])
		i += sz + 1
	}

	return res
}

func appendKey(key []byte, name []byte) []byte {
	key = append(key, byte(len(name)))
	key = append(key, name...)
	return key
}

// primaryBucketName returns <CID>.
func primaryBucketName(cid *cid.ID) []byte {
	return cidBucketKey(cid, primaryPrefix, nil)
}

// tombstoneBucketName returns <CID>_TS.
func tombstoneBucketName(cid *cid.ID) []byte {
	return cidBucketKey(cid, tombstonePrefix, nil)
}

// storageGroupBucketName returns <CID>_SG.
func storageGroupBucketName(cid *cid.ID) []byte {
	return cidBucketKey(cid, storageGroupPrefix, nil)
}

// parentBucketName returns <CID>_parent.
func parentBucketName(cid *cid.ID) []byte {
	return cidBucketKey(cid, parentPrefix, nil)
}

// addressKey returns key for K-V tables when key is a whole address.
func addressKey(addr *object.Address) []byte {
	return []byte(addr.String())
}

// parses object address formed by addressKey.
func addressFromKey(k []byte) (*object.Address, error) {
	a := object.NewAddress()
	return a, a.Parse(string(k))
}

// objectKey returns key for K-V tables when key is an object id.
func objectKey(oid *object.ID) []byte {
	return []byte(oid.String())
}
