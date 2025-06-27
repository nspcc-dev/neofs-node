package putsvc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/klauspost/reedsolomon"
	combinations "github.com/mxschmitt/golang-combinations"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessiontest "github.com/nspcc-dev/neofs-sdk-go/session/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
)

type inMemObjects struct {
	list []object.Object
}

func (x *inMemObjects) WriteHeader(hdr *object.Object) error {
	// TODO: For linker, payload appears twice: here in header, and in Write. It's better to avoid doing this.
	hdr = hdr.CutPayload()

	// TODO: It's easy to forget, and then the data will be mutated. Try not to require data flush.
	var cp object.Object
	hdr.CopyTo(&cp)

	x.list = append(x.list, cp)
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

func combos(n, k int) iter.Seq[[]int] {
	s := make([]int, n)
	for i := range s {
		s[i] = i
	}
	return slices.Values(combinations.Combinations(s, k))
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

func getAttribute(s []object.Attribute, k string) string {
	if ind := slices.IndexFunc(s, func(e object.Attribute) bool { return e.Key() == k }); ind >= 0 {
		return s[ind].Value()
	}
	return ""
}

func assertTZHash(t *testing.T, must bool, obj object.Object) {
	cs, ok := obj.PayloadHomomorphicHash()
	require.Equal(t, must, ok)
	if must {
		calc := tz.Sum(obj.Payload())
		require.Equal(t, calc[:], cs.Value())
	}
}

func recoverPayloadFromReedSolomonShards(t *testing.T, ln uint64, rsp reedSolomonPolicy, shardObjs []object.Object) []byte {
	var shards [][]byte
	for i := range shardObjs {
		shards = append(shards, shardObjs[i].Payload())
	}

	if ln == 0 {
		ind := slices.IndexFunc(shards, func(e []byte) bool { return len(e) > 0 })
		require.Negative(t, ind, "non-empty payload from empty source object in Reed-Solomon shard")
		return nil
	}

	enc, err := reedsolomon.New(rsp.dataShards, rsp.parityShards)
	require.NoError(t, err)

	ok, err := enc.Verify(shards)
	require.NoError(t, err)
	require.True(t, ok)

	required := make([]bool, rsp.dataShards+rsp.parityShards)
	for i := range rsp.dataShards {
		required[i] = true
	}

	for lostCount := 1; lostCount <= rsp.parityShards; lostCount++ {
		for lostIdxs := range combos(len(shards), lostCount) {
			shardsCp := excludeShards(shards, lostIdxs)
			require.NoError(t, enc.Reconstruct(shardsCp),
				"source payload cannot be recovered from Reed-Solomon shards when no more than %d items lost (%v)", rsp.parityShards, lostIdxs)
			require.Equal(t, shards, shardsCp, "wrong payload recovered without an error")

			shardsCp = excludeShards(shards, lostIdxs)
			require.NoError(t, enc.ReconstructSome(shardsCp, required),
				"source payload cannot be recovered from Reed-Solomon shards when no more than %d items lost (%v)", rsp.parityShards, lostIdxs)
			require.Equal(t, shards[:rsp.dataShards], shardsCp[:rsp.dataShards], "wrong payload recovered without an error")
		}
	}

	for lostIdxs := range combos(len(shards), rsp.parityShards+1) {
		require.Error(t, enc.Reconstruct(excludeShards(shards, lostIdxs)),
			"source payload can be recovered from Reed-Solomon shards when more than %d items lost (%v)", rsp.parityShards, lostIdxs)
		require.Error(t, enc.ReconstructSome(excludeShards(shards, lostIdxs), required),
			"source payload can be recovered from Reed-Solomon shards when more than %d items lost (%v)", rsp.parityShards, lostIdxs)
	}

	data := make([]byte, 0, ln)
	for i := range shards[:rsp.dataShards] {
		data = append(data, shardObjs[i].Payload()...)
	}

	require.GreaterOrEqual(t, uint64(len(data)), ln, "source payload is truncated in Reed-Solomon shards")

	require.False(t, slices.ContainsFunc(data[ln:], func(b byte) bool { return b != 0 }),
		"source payload is aligned with non-zero byte in Reed-Solomon shards")

	return data[:ln]
}

func recoverHeaderFromReedSolomonShard(t *testing.T, shard object.Object) (object.Object, int) {
	attrs := shard.Attributes()

	idxAttr := getAttribute(attrs, "__NEOFS__EC_RS_IDX")
	require.NotEmpty(t, idxAttr, "missing attribute for shard index in Reed-Solomon shard")
	idx, err := strconv.Atoi(idxAttr)
	require.NoError(t, err, "attribute for shard index contains invalid int in Reed-Solomon shard")
	require.True(t, idx >= 0, "attribute for shard index contains negative value in Reed-Solomon shard")

	srcPldLenAttr := getAttribute(attrs, "__NEOFS__EC_RS_SRC_PAYLOAD_LEN")
	require.NotEmpty(t, srcPldLenAttr, "missing attribute for source payload len in Reed-Solomon shard")
	srcPldLen, err := strconv.ParseUint(srcPldLenAttr, 10, 64)
	require.NoError(t, err, "attribute for source payload len attribute contains invalid uint in Reed-Solomon shard")

	srcPldSHA256Attr := getAttribute(attrs, "__NEOFS__EC_RS_SRC_PAYLOAD_HASH_SHA256")
	require.NotEmpty(t, srcPldSHA256Attr, "missing attribute for source payload SHA256 hash in Reed-Solomon shard")
	srcPldSHA256, err := hex.DecodeString(srcPldSHA256Attr)
	require.NoError(t, err, "attribute for source payload SHA256 hash contains invalid HEX in Reed-Solomon shard")
	require.Len(t, srcPldSHA256, sha256.Size, "attribute for source payload SHA256 hash contains wrong len value in Reed-Solomon shard")

	srcPldTZAttr := getAttribute(attrs, "__NEOFS__EC_RS_SRC_PAYLOAD_HASH_TZ")
	var srcPldTZ []byte
	if srcPldTZAttr != "" {
		srcPldTZ, err = hex.DecodeString(srcPldTZAttr)
		require.NoError(t, err, "attribute for source payload TZ hash contains invalid HEX in Reed-Solomon shard")
		require.Len(t, srcPldTZ, tz.Size, "attribute for source payload TZ hash contains wrong len value in Reed-Solomon shard")
	}

	idAttr := getAttribute(attrs, "__NEOFS__EC_RS_SRC_ID")
	require.NotEmpty(t, idAttr, "missing attribute for source object ID in Reed-Solomon shard")
	var id oid.ID
	require.NoError(t, id.DecodeString(idAttr), "attribute for source object ID contains invalid value in Reed-Solomon shard")

	srcSigAttr := getAttribute(attrs, "__NEOFS__EC_RS_SRC_SIGNATURE")
	require.NotEmpty(t, srcSigAttr, "missing attribute for source object signature in Reed-Solomon shard")
	binSig, err := base64.StdEncoding.DecodeString(srcSigAttr)
	require.NoError(t, err, "attribute for source object signature contains invalid Base-64 in Reed-Solomon shard")
	var srcSig neofscrypto.Signature
	require.NoError(t, srcSig.Unmarshal(binSig), "attribute for source object signature contains invalid BLOB in Reed-Solomon shard")

	attrs = slices.DeleteFunc(attrs, func(a object.Attribute) bool {
		return strings.HasPrefix(a.Key(), "__NEOFS__EC_RS_")
	})

	srcHdr := *shard.CutPayload()
	srcHdr.SetPayloadSize(srcPldLen)
	srcHdr.SetPayloadChecksum(checksum.New(checksum.SHA256, srcPldSHA256))
	if srcPldTZAttr != "" {
		srcHdr.SetPayloadHomomorphicHash(checksum.New(checksum.TillichZemor, srcPldTZ))
	}
	srcHdr.SetAttributes(attrs...)
	srcHdr.SetID(id)
	srcHdr.SetSignature(&srcSig)

	return srcHdr, idx
}

func recoverObjectFromReedSolomonShards(t *testing.T, pldLenLimit uint64, withTZHash bool, rsp reedSolomonPolicy, shards []object.Object) object.Object {
	for i, shard := range shards {
		require.LessOrEqual(t, shard.PayloadSize(), pldLenLimit, "payload overflows len limit in Reed-Solomon shard #%d", i)
		require.EqualValues(t, len(shard.Payload()), shard.PayloadSize(), "wrong payload length in Reed-Solomon shard #%d", i)
		assertTZHash(t, withTZHash, shard)
		require.NoError(t, shard.CheckVerificationFields(), "invalid object for Reed-Solomon shard #%d", i)
	}

	var srcHdr object.Object
	for i := range shards {
		srcHdrI, idx := recoverHeaderFromReedSolomonShard(t, shards[i])
		require.EqualValues(t, idx, i, "order of shards is broken in Reed-Solomon shards")

		if i == 0 {
			srcHdr = srcHdrI
		} else {
			require.Equal(t, srcHdr, srcHdrI, "different source object headers in Reed-Solomon shard")
		}
	}

	srcPld := recoverPayloadFromReedSolomonShards(t, srcHdr.PayloadSize(), rsp, shards)

	srcObj := srcHdr
	srcObj.SetPayload(srcPld)

	return srcObj
}

func verifySplitShards(t *testing.T, pldLenLimit uint64, withTZHash bool, shards []object.Object) {
	for i, shard := range shards {
		require.LessOrEqual(t, shard.PayloadSize(), pldLenLimit, "payload overflows len limit in split shard #%d", i)
		require.EqualValues(t, len(shard.Payload()), shard.PayloadSize(), "wrong payload length in split shard #%d", i)
		assertTZHash(t, withTZHash, shard)
		require.NoError(t, shard.CheckVerificationFields(), "invalid object for split shard #%d", i)
	}

	if len(shards) == 1 {
		require.Zero(t, shards[0].GetParentID(), "parent ID in unsplit object")
		require.Nil(t, shards[0].Parent(), "parent header in unsplit object")
		return
	}

	require.GreaterOrEqual(t, len(shards), 3, "less than 3 split shards") // at least two shard + linker

	dataShards := shards[:len(shards)-1] // linker is last

	parHdrInLast := dataShards[len(dataShards)-1].Parent()
	require.NotZero(t, parHdrInLast, "missing parent header in last split shard")
	require.NotZero(t, parHdrInLast.GetID(), "missing parent object ID in last split shard")
	require.NotZero(t, parHdrInLast.Signature(), "missing parent object signature in last split shard")

	partHdrInFirst := dataShards[0].Parent()
	require.NotZero(t, partHdrInFirst, "missing parent header in first split shard")
	require.Zero(t, partHdrInFirst.GetID(), "parent object ID is set while should not be in first split shard")
	require.Zero(t, partHdrInFirst.Signature(), "parent object signature is set while should not be in first split shard")

	ind := slices.IndexFunc(dataShards[1:len(dataShards)-1], func(shard object.Object) bool { return !shard.GetParentID().IsZero() || shard.Parent() != nil })
	require.Negative(t, ind, "parent header is set while should not be in intermediate split shards")

	parCnr := parHdrInLast.GetContainerID()
	require.Equal(t, parCnr, partHdrInFirst.GetContainerID(), "different parent containers in first and last split shards")
	ind = slices.IndexFunc(shards, func(shard object.Object) bool { return shard.GetContainerID() != parCnr })
	require.Negative(t, ind, "wrong container in split shard")

	parOwner := parHdrInLast.Owner()
	require.Equal(t, parOwner, partHdrInFirst.Owner(), "different parent owners in first and last split shards")
	ind = slices.IndexFunc(shards, func(shard object.Object) bool { return shard.Owner() != parOwner })
	require.Negative(t, ind, "wrong owner in split shard")

	ind = slices.IndexFunc(shards, func(shard object.Object) bool { return len(shard.Attributes()) > 0 })
	require.Negative(t, ind, "attributes are set in split shard")

	require.Zero(t, dataShards[0].GetFirstID(), "first shard ID is set while should not be in first split shard")
	require.Zero(t, dataShards[0].GetPreviousID(), "previous shard ID is set while should not be in first split shard")

	for i := range dataShards[1:] {
		require.Equal(t, dataShards[i].GetID(), dataShards[i+1].GetPreviousID(), "wrong previous shard ID in split shard #%d", i)
		require.Equal(t, dataShards[0].GetID(), dataShards[i+1].GetFirstID(), "wrong first shard ID in split shard #%d", i)
	}

	linker := shards[len(shards)-1]
	require.Equal(t, linker.Parent(), parHdrInLast, "different parent headers in last and link split shards")
	require.Equal(t, object.TypeLink, linker.Type(), "wrong object type in link split shard")

	require.NotEmpty(t, linker.Payload(), "empty payload in link split shard")
	var linkerData object.Link
	require.NoError(t, linkerData.Unmarshal(linker.Payload()), "invalid payload in link split shard")
	linkerObjs := linkerData.Objects()
	require.Len(t, linkerObjs, len(dataShards), "wrong number of items in link split shard")
	for i := range linkerObjs {
		require.Equal(t, dataShards[i].GetID(), linkerObjs[i].ObjectID(), "wrong shard ID in link split shard item #%d", i)
		require.EqualValues(t, dataShards[i].PayloadSize(), linkerObjs[i].ObjectSize(), "wrong shard payload len in link split shard item #%d", i)
	}
}

func recoverObjectFromSplitShards(t *testing.T, pldLenLimit uint64, withTZHash bool, shards []object.Object) object.Object {
	verifySplitShards(t, pldLenLimit, withTZHash, shards)

	if len(shards) == 1 {
		return shards[0]
	}

	var srcPld []byte
	for _, shard := range shards[:len(shards)-1] { // cut linker
		srcPld = append(srcPld, shard.Payload()...)
	}

	srcObj := *shards[len(shards)-2].Parent()
	srcObj.SetPayload(srcPld)

	return srcObj
}

func recoverSplitObjectFromReedSolomonShards(t *testing.T, pldLenLimit uint64, withTZHash bool, rsp reedSolomonPolicy, rsShards []object.Object) object.Object {
	rsShardsPerObject := rsp.dataShards + rsp.parityShards

	var splitShards []object.Object
	for s := range slices.Chunk(rsShards, rsShardsPerObject) {
		splitShard := recoverObjectFromReedSolomonShards(t, pldLenLimit, withTZHash, rsp, s)
		splitShards = append(splitShards, splitShard)
	}

	srcObj := recoverObjectFromSplitShards(t, pldLenLimit, withTZHash, splitShards)

	return srcObj
}

func splitAndRecoverObject(t *testing.T, srcObj object.Object, pldLenLimit uint64, withTZHash bool, st *session.Object, rsp reedSolomonPolicy) object.Object {
	const curEpoch = 123
	signer := usertest.User()

	var objs inMemObjects
	slcr := newSlicingTarget(context.Background(), pldLenLimit, !withTZHash, signer, st, curEpoch, &objs, &rsp)

	srcObj.SetOwner(signer.ID)

	err := slcr.WriteHeader(srcObj.CutPayload())
	require.NoError(t, err)
	_, err = slcr.Write(srcObj.Payload())
	require.NoError(t, err)
	id, err := slcr.Close()
	require.NoError(t, err)

	recvObj := recoverSplitObjectFromReedSolomonShards(t, pldLenLimit, withTZHash, rsp, objs.list)

	require.Equal(t, id, recvObj.GetID())
	require.NotNil(t, recvObj.Version())
	require.Equal(t, version.Current(), *recvObj.Version())
	require.EqualValues(t, len(recvObj.Payload()), recvObj.PayloadSize(), "wrong payload length in recovered object")
	require.NoError(t, recvObj.CheckVerificationFields(), "recovered object is invalid")
	assertTZHash(t, withTZHash, recvObj)
	require.EqualValues(t, curEpoch, recvObj.CreationEpoch())
	if st != nil {
		require.NotNil(t, recvObj)
		require.Equal(t, st, recvObj.SessionToken())
		require.Equal(t, st.Issuer(), recvObj.Owner())
	} else {
		require.Nil(t, recvObj.SessionToken())
		require.Equal(t, srcObj.Owner(), recvObj.Owner())
	}

	return recvObj
}

func testReedSolomonPolicy(t *testing.T, pldLenLimit, pldLen uint64, rsp reedSolomonPolicy) {
	cnr := cidtest.ID()
	payload := testutil.RandByteSlice(pldLen)
	attrs := []object.Attribute{
		object.NewAttribute("attr_1", "val_1"),
		object.NewAttribute("attr_2", "val_2"),
	}

	var srcObj object.Object
	srcObj.SetContainerID(cnr)
	srcObj.SetAttributes(attrs...)
	srcObj.SetPayload(payload)

	check := func(t *testing.T, recvObj object.Object) {
		require.Equal(t, cnr, recvObj.GetContainerID())
		require.Equal(t, attrs, recvObj.Attributes())
		require.True(t, bytes.Equal(payload, recvObj.Payload()))
	}

	t.Run("with TZ hash", func(t *testing.T) {
		recvObj := splitAndRecoverObject(t, srcObj, pldLenLimit, true, nil, rsp)
		check(t, recvObj)
	})

	t.Run("with session token", func(t *testing.T) {
		tok := sessiontest.Object()
		recvObj := splitAndRecoverObject(t, srcObj, pldLenLimit, false, &tok, rsp)
		check(t, recvObj)
	})

	recvObj := splitAndRecoverObject(t, srcObj, pldLenLimit, false, nil, rsp)
	check(t, recvObj)
}

func TestReedSolomon(t *testing.T) {
	const maxObjectSize = 4 << 10

	for _, rsp := range []reedSolomonPolicy{
		{dataShards: 3, parityShards: 1},
		{dataShards: 6, parityShards: 3},
		{dataShards: 12, parityShards: 4},
	} {
		t.Run(fmt.Sprintf("(%d,%d)", rsp.dataShards, rsp.parityShards), func(t *testing.T) {
			t.Run("empty payload", func(t *testing.T) {
				testReedSolomonPolicy(t, maxObjectSize, 0, rsp)
			})
			t.Run("one byte", func(t *testing.T) {
				testReedSolomonPolicy(t, maxObjectSize, 1, rsp)
			})
			t.Run("limit-1", func(t *testing.T) {
				testReedSolomonPolicy(t, maxObjectSize, maxObjectSize-1, rsp)
			})
			t.Run("exactly limit", func(t *testing.T) {
				testReedSolomonPolicy(t, maxObjectSize, maxObjectSize, rsp)
			})
			t.Run("limit+1", func(t *testing.T) {
				testReedSolomonPolicy(t, maxObjectSize, maxObjectSize+1, rsp)
			})
			t.Run("limit+50%", func(t *testing.T) {
				testReedSolomonPolicy(t, maxObjectSize, maxObjectSize+maxObjectSize/2, rsp)
			})
			t.Run("limitX2", func(t *testing.T) {
				testReedSolomonPolicy(t, maxObjectSize, maxObjectSize*2, rsp)
			})
			t.Run("limitX5+1", func(t *testing.T) {
				testReedSolomonPolicy(t, maxObjectSize, maxObjectSize*5+1, rsp)
			})
		})
	}
}
