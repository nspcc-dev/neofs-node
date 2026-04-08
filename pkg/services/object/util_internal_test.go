package object

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

func messageToSingleMemBuffer(t testing.TB, m proto.Message) mem.BufferSlice {
	buf, err := proto.Marshal(m)
	require.NoError(t, err)
	return mem.BufferSlice{mem.SliceBuffer(buf)}
}

func nestMetaHeader(m *protosession.ResponseMetaHeader, n int) *protosession.ResponseMetaHeader {
	for range n {
		st := m.GetStatus()
		m = &protosession.ResponseMetaHeader{
			Version:  m.Version,
			Epoch:    m.Epoch,
			Ttl:      m.Ttl,
			XHeaders: m.XHeaders,
			Origin:   m,
			Status: &protostatus.Status{
				Code:    st.GetCode() + 1,
				Message: st.GetMessage(),
				Details: st.GetDetails(),
			},
		}
	}
	return m
}

func newBlankMetaHeader() *protosession.ResponseMetaHeader {
	return &protosession.ResponseMetaHeader{
		Version: &protorefs.Version{
			Major: 3651676384,
			Minor: 2829345803,
		},
		Epoch: 10699904184716558895,
		Ttl:   1657590594,
		XHeaders: []*protosession.XHeader{
			{Key: "key1", Value: ""},
			{Key: "", Value: "value2"},
			{Key: "key3", Value: "value3"},
			{Key: "", Value: ""},
		},
	}
}

func newTestObject(t testing.TB) object.Object {
	var tokV1 session.Object
	tokV1.SetID(uuid.New())
	tokV1.SetIssuer(usertest.ID())
	tokV1.SetAuthKey(neofscryptotest.Signer().Public())
	tokV1.SetExp(17170611258521075862)
	tokV1.SetNbf(738661496128559041)
	tokV1.SetIat(4380256499670963239)
	tokV1.ForVerb(session.VerbObjectHead)
	tokV1.BindContainer(cidtest.ID())
	tokV1.LimitByObjects(oidtest.IDs(10)...)
	require.NoError(t, tokV1.Sign(usertest.User()))

	originSessionToken := newTestUnsignedSessionToken(t)
	require.NoError(t, originSessionToken.Sign(usertest.User()))

	sessionToken := newTestUnsignedSessionToken(t)
	sessionToken.SetOrigin(&originSessionToken)
	require.NoError(t, sessionToken.Sign(usertest.User()))

	par := *object.New(cidtest.ID(), usertest.ID())
	par.SetCreationEpoch(10738628592919807909)
	par.SetPayloadSize(12653732379852397698)
	par.SetPayloadHomomorphicHash(checksum.NewTillichZemor([64]byte(testutil.RandByteSlice(64))))
	par.SetType(object.TypeLink)
	par.SetPayload(testutil.RandByteSlice(1 << 10))
	par.SetAttributes(
		object.NewAttribute("pk1", "pv1"),
		object.NewAttribute("pk2", "pv2"),
	)
	par.SetSessionToken(&tokV1)
	par.SetSessionTokenV2(&sessionToken)
	require.NoError(t, par.SetVerificationFields(usertest.User()))

	obj := *object.New(cidtest.ID(), usertest.ID())
	obj.SetCreationEpoch(18093843418564698707)
	obj.SetPayloadSize(895946572232418931)
	obj.SetPayloadHomomorphicHash(checksum.NewTillichZemor([64]byte(testutil.RandByteSlice(64))))
	obj.SetType(object.TypeLink)
	obj.SetPayload(testutil.RandByteSlice(1 << 10))
	obj.SetAttributes(
		object.NewAttribute("k1", "v1"),
		object.NewAttribute("k2", "v2"),
	)
	obj.SetSessionToken(&tokV1)
	obj.SetSessionTokenV2(&sessionToken)
	obj.SetParent(&par)
	require.NoError(t, obj.SetVerificationFields(usertest.User()))

	return obj
}

func newTestUnsignedSessionToken(t testing.TB) sessionv2.Token {
	var tok sessionv2.Token
	tok.SetVersion(2357623054)
	tok.SetAppData(testutil.RandByteSlice(1024))
	tok.SetIssuer(usertest.ID())
	tm := time.Unix(1774601168, 0)
	tok.Lifetime = sessionv2.NewLifetime(tm, tm.Add(time.Minute), tm.Add(time.Hour))

	ctx1, err := sessionv2.NewContext(cidtest.ID(), []sessionv2.Verb{sessionv2.VerbObjectHead, sessionv2.VerbObjectGet})
	require.NoError(t, err)
	require.NoError(t, tok.AddContext(ctx1))
	ctx2, err := sessionv2.NewContext(cidtest.ID(), []sessionv2.Verb{sessionv2.VerbObjectPut, sessionv2.VerbObjectDelete})
	require.NoError(t, err)
	require.NoError(t, tok.AddContext(ctx2))

	require.NoError(t, tok.AddSubject(sessionv2.NewTargetUser(usertest.ID())))
	require.NoError(t, tok.AddSubject(sessionv2.NewTargetNamed("Bob")))

	return tok
}

func newTestSplitInfo() *protoobject.SplitInfo {
	var res object.SplitInfo
	res.SetSplitID(object.NewSplitID())
	res.SetLastPart(oidtest.ID())
	res.SetLink(oidtest.ID())
	res.SetFirstPart(oidtest.ID())
	return res.ProtoMessage()
}

func newAnyVerificationHeader() *protosession.ResponseVerificationHeader {
	return &protosession.ResponseVerificationHeader{
		BodySignature: &protorefs.Signature{
			Key:    []byte("any_body_key"),
			Sign:   []byte("any_body_signature"),
			Scheme: 123,
		},
		MetaSignature: &protorefs.Signature{
			Key:    []byte("any_meta_key"),
			Sign:   []byte("any_meta_signature"),
			Scheme: 456,
		},
		OriginSignature: &protorefs.Signature{
			Key:    []byte("any_origin_key"),
			Sign:   []byte("any_origin_signature"),
			Scheme: 789,
		},
		Origin: &protosession.ResponseVerificationHeader{
			BodySignature: &protorefs.Signature{
				Key:    []byte("any_origin_body_key"),
				Sign:   []byte("any_origin_body_signature"),
				Scheme: 321,
			},
			MetaSignature: &protorefs.Signature{
				Key:    []byte("any_origin_meta_key"),
				Sign:   []byte("any_origin_meta_signature"),
				Scheme: 654,
			},
			OriginSignature: &protorefs.Signature{
				Key:    []byte("any_origin_origin_key"),
				Sign:   []byte("any_origin_origin_signature"),
				Scheme: 987,
			},
		},
	}
}
