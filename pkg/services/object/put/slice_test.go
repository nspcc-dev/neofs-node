package putsvc

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/hex"
	"iter"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/klauspost/reedsolomon"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/stat/combin"
)

const (
	maxObjectSize = 4 << 10
	currentEpoch  = 123
)

var (
	ctx          = context.Background()
	anyOwner     = usertest.User()
	anyOwnerID   = anyOwner.UserID()
	anyContainer = cidtest.ID()
)

type inMemObjects struct {
	list []object.Object
}

func (x *inMemObjects) WriteHeader(hdr *object.Object) error {
	x.list = append(x.list, *hdr)
	return nil
}

func (x *inMemObjects) Write(p []byte) (int, error) {
	last := &x.list[len(x.list)-1]
	last.SetPayload(append(last.Payload(), p...))
	return len(p), nil
}

func (x *inMemObjects) Close() (oid.ID, error) {
	return x.list[len(x.list)-1].GetID(), nil
}

func assertAllObjects(t *testing.T, objs []object.Object, msg string, f func(object.Object) bool) {
	ind := slices.IndexFunc(objs, func(obj object.Object) bool { return !f(obj) })
	require.Negative(t, ind, msg)
}

func combinations(n, k int) iter.Seq[[]int] {
	// TODO: replace
	return slices.Values(combin.Combinations(n, k))
}

func excludeShards(shards [][]byte, idsx []int) [][]byte {
	shardsCp := slices.Clone(shards)
	for i, shard := range shardsCp {
		shardsCp[i] = slices.Clone(shard)
	}
	for _, idx := range idsx {
		shardsCp[idx] = nil
	}
	return shardsCp
}

func getAttribute(obj object.Object, attr string) string {
	attrs := obj.Attributes()
	if ind := slices.IndexFunc(attrs, func(a object.Attribute) bool { return a.Key() == attr }); ind >= 0 {
		return attrs[ind].Value()
	}
	return ""
}

func assertReedSolomonShardIndexRecovery(t *testing.T, all int, shard object.Object) int {
	strIdx := getAttribute(shard, "__NEOFS__EC_RS_IDX")
	require.NotEmpty(t, strIdx, "missing Reed-Solomon shard index attribute in produced object")

	idx, err := strconv.Atoi(strIdx)
	require.NoError(t, err, "Reed-Solomon shard index attribute is not a valid integer in produced object")
	require.True(t, idx >= 0 && idx < all, "Reed-Solomon shard index attribute out of range in produced object")

	return idx
}

func assertReedSolomonShardOrderRecovery(t *testing.T, shuffled []object.Object) {
	slices.SortFunc(shuffled, func(a, b object.Object) int {
		aIdx := assertReedSolomonShardIndexRecovery(t, len(shuffled), a)
		bIdx := assertReedSolomonShardIndexRecovery(t, len(shuffled), b)
		return cmp.Compare(aIdx, bIdx)
	})
}

func assertPayloadLenRecoveryFromReedSolomonShards(t *testing.T, shardObjs []object.Object) uint64 {
	strPayloadLen := getAttribute(shardObjs[0], "__NEOFS__EC_RS_SRC_PAYLOAD_LEN")
	require.NotEmpty(t, strPayloadLen, "missing source payload len attribute in produced object")

	payloadLen, err := strconv.ParseUint(strPayloadLen, 10, 64)
	require.NoError(t, err, "invalid source payload len attribute in produced object")

	ind := slices.IndexFunc(shardObjs[1:], func(obj object.Object) bool {
		return getAttribute(obj, "__NEOFS__EC_RS_SRC_PAYLOAD_LEN") != strPayloadLen
	})
	require.Negative(t, ind, "different source payload len attributes in produced objects")

	return payloadLen
}

func assertPayloadChecksumRecoveryFromReedSolomonShards(t *testing.T, shardObjs []object.Object) checksum.Checksum {
	strPldHash := getAttribute(shardObjs[0], "__NEOFS__EC_RS_SRC_PAYLOAD_HASH")
	require.NotEmpty(t, strPldHash, "missing source payload hash attribute in produced object")

	pldHash, err := hex.DecodeString(strPldHash)
	require.NoError(t, err, "source payload hash attribute is not a valid HEX in produced object")

	ind := slices.IndexFunc(shardObjs[1:], func(obj object.Object) bool {
		return getAttribute(obj, "__NEOFS__EC_RS_SRC_PAYLOAD_HASH") != strPldHash
	})
	require.Negative(t, ind, "different source payload hash attributes in produced objects")

	return checksum.New(checksum.SHA256, pldHash)
}

func assertSignatureRecoveryFromReedSolomonShards(t *testing.T, shardObjs []object.Object) neofscrypto.Signature {
	strSig := getAttribute(shardObjs[0], "__NEOFS__EC_RS_SRC_SIGNATURE")
	require.NotEmpty(t, strSig, "missing source signature attribute in produced object")

	binSig, err := base64.StdEncoding.DecodeString(strSig)
	require.NoError(t, err, "source signature attribute is not a valid Base-64 in produced object")

	var sig neofscrypto.Signature
	require.NoError(t, sig.Unmarshal(binSig), "invalid binary source signature attribute in produced object")

	ind := slices.IndexFunc(shardObjs[1:], func(obj object.Object) bool {
		return getAttribute(obj, "__NEOFS__EC_RS_SRC_SIGNATURE") != strSig
	})
	require.Negative(t, ind, "different source signature attributes in produced objects")

	return sig
}

func assertPayloadRecoveryFromReedSolomonShards(t *testing.T, rsp reedSolomonPrm, shardObjs []object.Object) []byte {
	enc, err := reedsolomon.New(rsp.dataShards, rsp.parityShards)
	require.NoError(t, err)

	var shards [][]byte
	for i := range shardObjs {
		shards = append(shards, shardObjs[i].Payload())
	}

	ok, err := enc.Verify(shards)
	require.NoError(t, err)
	require.True(t, ok)

	required := make([]bool, rsp.dataShards+rsp.parityShards)
	for i := range rsp.dataShards {
		required[i] = true
	}

	for lostCount := 1; lostCount <= rsp.parityShards; lostCount++ {
		for lostIdxs := range combinations(len(shards), lostCount) {
			shardsCp := excludeShards(shards, lostIdxs)
			require.NoError(t, enc.Reconstruct(shardsCp),
				"unrecoverable shards with no more %d items lost (%v)", rsp.parityShards, lostIdxs)
			require.Equal(t, shards, shardsCp, "wrong data recovered without error")

			shardsCp = excludeShards(shards, lostIdxs)
			require.NoError(t, enc.ReconstructSome(shardsCp, required),
				"unrecoverable data shards with no more %d items lost (%v)", rsp.parityShards, lostIdxs)
			require.Equal(t, shards[:rsp.dataShards], shardsCp[:rsp.dataShards], "wrong data recovered without error")
		}
	}

	for lostIdxs := range combinations(len(shards), rsp.parityShards+1) {
		require.ErrorIs(t, enc.Reconstruct(excludeShards(shards, lostIdxs)), reedsolomon.ErrTooFewShards,
			"recoverable shards with more than %d items lost (%v)", rsp.parityShards, lostIdxs)
		require.ErrorIs(t, enc.ReconstructSome(excludeShards(shards, lostIdxs), required), reedsolomon.ErrTooFewShards,
			"recoverable data shards with more than %d items lost (%v)", rsp.parityShards, lostIdxs)
	}

	srcPayloadLen := assertPayloadLenRecoveryFromReedSolomonShards(t, shardObjs)

	collectedData := make([]byte, 0, srcPayloadLen)
	for i := range shards[:rsp.dataShards] {
		collectedData = append(collectedData, shardObjs[i].Payload()...)
	}

	require.GreaterOrEqual(t, uint64(len(collectedData)), srcPayloadLen, "original data is truncated in RS data shards")
	require.False(t, slices.ContainsFunc(collectedData[srcPayloadLen:], func(b byte) bool { return b != 0 }),
		"last RS data shard aligned with non-zero byte")

	return collectedData[:srcPayloadLen]
}

func cutSourceHeaderFromReedSolomonShard(shard object.Object) object.Object {
	shard = *shard.CutPayload()
	shard.ResetID()
	shard.SetPayloadChecksum(checksum.Checksum{})
	shard.SetSignature(nil)
	// shard.SetPayloadHomomorphicHash(checksum.Checksum{})

	attrs := shard.Attributes()
	attrs = attrs[:slices.IndexFunc(attrs, func(attr object.Attribute) bool {
		return strings.HasPrefix(attr.Key(), "__NEOFS__EC_RS")
	})]
	shard.SetAttributes(attrs...)

	return shard
}

func assertHeaderRecoveryFromReedSolomonShards(t *testing.T, shardObjs []object.Object) object.Object {
	srcPayloadLen := assertPayloadLenRecoveryFromReedSolomonShards(t, shardObjs)
	srcPayloadHash := assertPayloadChecksumRecoveryFromReedSolomonShards(t, shardObjs)
	srcSignature := assertSignatureRecoveryFromReedSolomonShards(t, shardObjs)

	hdr := cutSourceHeaderFromReedSolomonShard(shardObjs[0])

	for i := range shardObjs[1:] {
		hdrI := cutSourceHeaderFromReedSolomonShard(shardObjs[i+1])
		require.Equal(t, hdr, hdrI, "different source headers in produced Reed-Solomon shards")
	}

	hdr.SetPayloadSize(srcPayloadLen)
	hdr.SetPayloadChecksum(srcPayloadHash)
	hdr.SetSignature(&srcSignature)

	return hdr
}

func assertObjectRecoveryFromReedSolomonShards(t *testing.T, rsp reedSolomonPrm, shardObjs []object.Object) object.Object {
	rand.Shuffle(len(shardObjs), func(i, j int) { shardObjs[i], shardObjs[j] = shardObjs[j], shardObjs[i] })
	assertReedSolomonShardOrderRecovery(t, shardObjs)

	recvHeader := assertHeaderRecoveryFromReedSolomonShards(t, shardObjs)
	recvPayload := assertPayloadRecoveryFromReedSolomonShards(t, rsp, shardObjs)

	recvObj := recvHeader
	recvObj.SetPayload(recvPayload)
	require.NoError(t, recvHeader.CheckVerificationFields(), "invalid object recovered from produced Reed-Solomon shards")

	return recvObj
}

func assertReedSolomonEncodingResult(t *testing.T, srcPayload []byte, withHomomorphicHash bool, sessionToken *session.Object, rsp reedSolomonPrm, res []object.Object) {
	assertAllObjects(t, res, "ID is missing or invalid in produced object", func(obj object.Object) bool {
		return obj.VerifyID() == nil
	})
	assertAllObjects(t, res, "signature is missing or invalid in produced object", func(obj object.Object) bool {
		return obj.VerifySignature()
	})
	assertAllObjects(t, res, "payload overflows len limit in produced object", func(obj object.Object) bool {
		return len(obj.Payload()) <= maxObjectSize
	})
	assertAllObjects(t, res, "API version header not equals current version in produced object", func(obj object.Object) bool {
		got := obj.Version()
		return got != nil && *got == version.Current()
	})
	assertAllObjects(t, res, "container ID header is missing or wrong in produced object", func(obj object.Object) bool {
		return obj.GetContainerID() == anyContainer
	})
	if sessionToken != nil {
		assertAllObjects(t, res, "owner ID header does not equal the session token issuer in produced object", func(obj object.Object) bool {
			return obj.Owner() == sessionToken.Issuer()
		})
	} else {
		assertAllObjects(t, res, "owner ID header is missing or wrong in produced object", func(obj object.Object) bool {
			return obj.Owner() == anyOwnerID
		})
	}
	assertAllObjects(t, res, "creation epoch header is missing or wrong in produced object", func(obj object.Object) bool {
		return obj.CreationEpoch() == currentEpoch
	})
	assertAllObjects(t, res, "payload len header mismatches payload in produced object", func(obj object.Object) bool {
		return obj.PayloadSize() == uint64(len(obj.Payload()))
	})
	assertAllObjects(t, res, "payload hash header is missing or mismatches payload in produced object", func(obj object.Object) bool {
		return obj.VerifyPayloadChecksum() == nil
	})
	assertAllObjects(t, res, "type header is not REGULAR in produced object", func(obj object.Object) bool {
		return obj.Type() == object.TypeRegular
	})
	if withHomomorphicHash {
		assertAllObjects(t, res, "homomorphic payload hash header is missing or mismatches payload in produced object", func(obj object.Object) bool {
			hdr, ok := obj.PayloadHomomorphicHash()
			calc := tz.Sum(obj.Payload())
			return ok && bytes.Equal(hdr.Value(), calc[:])
		})
	} else {
		assertAllObjects(t, res, "homomorphic payload hash header is set while should not be in produced object", func(obj object.Object) bool {
			_, ok := obj.PayloadHomomorphicHash()
			return !ok
		})
	}
	if sessionToken != nil {
		assertAllObjects(t, res, "session token header is missing or wrong in produced object", func(obj object.Object) bool {
			got := obj.SessionToken()
			return got != nil && bytes.Equal(got.Marshal(), sessionToken.Marshal())
		})
	} else {
		assertAllObjects(t, res, "session token header is set while should not be in produced object", func(obj object.Object) bool {
			return obj.SessionToken() == nil
		})
	}

	shardsPerObject := rsp.dataShards + rsp.parityShards
	expectedObjNum := 1
	if len(srcPayload) > maxObjectSize {
		expectedObjNum = len(srcPayload) / maxObjectSize
		if len(srcPayload)%maxObjectSize != 0 {
			expectedObjNum++
		}
	}
	require.EqualValues(t, expectedObjNum*shardsPerObject, len(res), "wrong number of produced objects")

	var recvObjs []object.Object
	for shards := range slices.Chunk(res, shardsPerObject) {
		recvObj := assertObjectRecoveryFromReedSolomonShards(t, rsp, shards)
		recvObjs = append(recvObjs, recvObj)
	}

	// shardGroups := len(res) / shardsPerObject
	// for i := range shardGroups {
	// 	shards := res[i*shardsPerObject:][:shardsPerObject]
	//
	// }
	// TODO: test payload
	// TODO: test headers
	// TODO: test recovery props (parity payloads)
	// TODO: for fixed data and RS prm, are parity blocks predictable? Consistency may be tested
}

func TestReedSolomon(t *testing.T) {
	var objs inMemObjects
	var sessionToken *session.Object
	const withHomomorphicHash = true

	rsp := reedSolomonPrm{
		dataShards:   4,
		parityShards: 2,
	}

	slcr := newSlicingTarget(ctx, maxObjectSize, !withHomomorphicHash, anyOwner, sessionToken, currentEpoch, &objs, &rsp)

	payload := testutil.RandByteSlice(maxObjectSize)

	var hdr object.Object
	hdr.SetContainerID(anyContainer)
	hdr.SetOwner(anyOwnerID)

	err := slcr.WriteHeader(&hdr)
	require.NoError(t, err)
	_, err = slcr.Write(payload)
	require.NoError(t, err)
	_, err = slcr.Close()
	require.NoError(t, err)

	assertReedSolomonEncodingResult(t, payload, withHomomorphicHash, sessionToken, rsp, objs.list)
}
