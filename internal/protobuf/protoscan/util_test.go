package protoscan_test

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
)

var (
	int32OverflowVarint  = []byte{128, 128, 128, 128, 8}                          // 2147483648
	uint32OverflowVarint = []byte{128, 128, 128, 128, 16}                         // 4294967296
	int64OverflowVarint  = []byte{128, 128, 128, 128, 128, 128, 128, 128, 128, 1} // 18446744073709551616
	uint64OverflowVarint = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 2} // 18446744073709551616
	negativeVarint       = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 1} // -1
)

var (
	tagLEN42    = []byte{210, 2}
	tagVARINT42 = []byte{208, 2}
)

func newSingleBufferSlice(buf []byte) iprotobuf.BuffersSlice {
	return iprotobuf.NewBuffersSlice(mem.BufferSlice{mem.SliceBuffer(buf)})
}

func byteByByteSlice(msg []byte) iprotobuf.BuffersSlice {
	buffers := make(mem.BufferSlice, 0, len(msg)*2)
	for i := range msg {
		// add intermediate empty buffers just to make sure they cause no problems
		buffers = append(buffers, mem.SliceBuffer{}, mem.SliceBuffer([]byte{msg[i]}))
	}

	return iprotobuf.NewBuffersSlice(buffers)
}

func newTestSessionToken(t testing.TB) sessionv2.Token {
	issuer := usertest.User()

	c1, err := sessionv2.NewContext(cidtest.ID(), []sessionv2.Verb{1, math.MaxInt32})
	require.NoError(t, err)
	c2, err := sessionv2.NewContext(cidtest.ID(), []sessionv2.Verb{math.MaxInt32, 2})
	require.NoError(t, err)

	var st sessionv2.Token

	st.SetVersion(math.MaxUint32)
	require.NoError(t, st.SetAppData(testutil.RandByteSlice(1024)))
	st.SetIssuer(issuer.ID)
	st.SetIat(time.Unix(1776341940, 0))
	st.SetNbf(time.Unix(1776341941, 0))
	st.SetExp(time.Unix(1776341942, 0))
	require.NoError(t, st.SetSubjects([]sessionv2.Target{
		sessionv2.NewTargetUser(usertest.ID()),
		sessionv2.NewTargetNamed("Bob"),
	}))
	require.NoError(t, st.SetContexts([]sessionv2.Context{c1, c2}))
	st.SetFinal(true)

	require.NoError(t, st.Sign(issuer))

	return st
}

func newTestComplexSessionToken(t testing.TB) sessionv2.Token {
	root := newTestSessionToken(t)

	inter := newTestSessionToken(t)
	inter.SetOrigin(&root)

	leaf := newTestSessionToken(t)
	leaf.SetOrigin(&inter)

	return leaf
}

func newTestObject(t testing.TB) object.Object {
	owner := usertest.User()

	ver := version.New(math.MaxUint32, math.MaxUint32)

	var stV1 session.Object
	stV1.SetID(uuid.New())
	stV1.SetIssuer(owner.ID)
	stV1.SetIat(math.MaxUint64)
	stV1.SetNbf(math.MaxUint64)
	stV1.SetExp(math.MaxUint64)
	stV1.SetAuthKey(neofscryptotest.Signer().Public())
	stV1.BindContainer(cidtest.ID())
	stV1.LimitByObjects(oidtest.IDs(3)...)
	require.NoError(t, stV1.Sign(owner))

	st := newTestComplexSessionToken(t)

	var obj object.Object
	obj.SetVersion(&ver)
	obj.SetContainerID(cidtest.ID())
	obj.SetOwner(owner.ID)
	obj.SetCreationEpoch(math.MaxUint64)
	obj.SetPayloadSize(math.MaxUint64)
	obj.SetPayloadChecksum(checksum.New(math.MaxInt32, testutil.RandByteSlice(32)))
	obj.SetType(math.MaxInt32)
	obj.SetPayloadHomomorphicHash(checksum.New(math.MaxInt32, testutil.RandByteSlice(64)))
	obj.SetSessionToken(&stV1)
	obj.SetAttributes(
		object.NewAttribute("hello", "world"),
		object.NewAttribute("foo", "bar"),
	)
	obj.SetPreviousID(oidtest.ID())
	obj.SetFirstID(oidtest.ID())
	obj.SetChildren(oidtest.IDs(3)...)
	obj.SetSplitID(object.NewSplitID())
	obj.SetParentID(oidtest.ID())
	obj.SetSessionTokenV2(&st)
	obj.SetPayload(testutil.RandByteSlice(1024))

	require.NoError(t, obj.SetIDWithSignature(owner))

	return obj
}

func newTestChildObject(t testing.TB) object.Object {
	par := newTestObject(t)

	child := newTestObject(t)
	child.SetParent(&par)

	return child
}
