package v2

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"

	bearertest "github.com/nspcc-dev/neofs-sdk-go/bearer/test"
	aclsdk "github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	protoacl "github.com/nspcc-dev/neofs-sdk-go/proto/acl"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	sessiontest "github.com/nspcc-dev/neofs-sdk-go/session/test"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/stretchr/testify/require"
)

func TestOriginalTokens(t *testing.T) {
	pk, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	signer := user.NewAutoIDSigner(*pk)

	sToken := sessiontest.ObjectSigned(signer)
	bToken := bearertest.Token()

	require.NoError(t, bToken.Sign(signer))

	// This line is needed because SDK uses some custom format for
	// reserved filters, so `cid.ID` is not converted to string immediately.
	require.NoError(t, bToken.FromProtoMessage(bToken.ProtoMessage()))

	mbt := bToken.ProtoMessage()
	mst := sToken.ProtoMessage()
	for i := range 10 {
		metaHeaders := testGenerateMetaHeader(uint32(i), mbt, mst)
		res, err := originalSessionToken(metaHeaders)
		require.NoError(t, err)
		require.Equal(t, sToken, *res, i)

		bTok, err := originalBearerToken(metaHeaders) //nolint:staticcheck // uncomment on unskip
		require.NoError(t, err)
		t.Skip("https://github.com/nspcc-dev/neofs-sdk-go/issues/606")
		require.Equal(t, &bToken, bTok, i)
	}
}

func testGenerateMetaHeader(depth uint32, b *protoacl.BearerToken, s *protosession.SessionToken) *protosession.RequestMetaHeader {
	metaHeader := new(protosession.RequestMetaHeader)
	metaHeader.BearerToken = b
	metaHeader.SessionToken = s

	for range depth {
		link := metaHeader
		metaHeader = new(protosession.RequestMetaHeader)
		metaHeader.Origin = link
	}

	return metaHeader
}

func TestIsVerbCompatible(t *testing.T) {
	// Source: https://nspcc.ru/upload/neofs-spec-latest.pdf#page=28
	table := map[aclsdk.Op][]sessionSDK.ObjectVerb{
		aclsdk.OpObjectPut:    {sessionSDK.VerbObjectPut},
		aclsdk.OpObjectDelete: {sessionSDK.VerbObjectDelete},
		aclsdk.OpObjectGet:    {sessionSDK.VerbObjectGet},
		aclsdk.OpObjectHead: {
			sessionSDK.VerbObjectHead,
			sessionSDK.VerbObjectGet,
			sessionSDK.VerbObjectDelete,
			sessionSDK.VerbObjectRange,
			sessionSDK.VerbObjectRangeHash,
		},
		aclsdk.OpObjectRange:  {sessionSDK.VerbObjectRange, sessionSDK.VerbObjectRangeHash},
		aclsdk.OpObjectHash:   {sessionSDK.VerbObjectRangeHash},
		aclsdk.OpObjectSearch: {sessionSDK.VerbObjectSearch, sessionSDK.VerbObjectDelete},
	}

	verbs := []sessionSDK.ObjectVerb{
		sessionSDK.VerbObjectPut,
		sessionSDK.VerbObjectDelete,
		sessionSDK.VerbObjectHead,
		sessionSDK.VerbObjectRange,
		sessionSDK.VerbObjectRangeHash,
		sessionSDK.VerbObjectGet,
		sessionSDK.VerbObjectSearch,
	}

	var tok sessionSDK.Object

	for op, list := range table {
		for _, verb := range verbs {
			var contains bool
			for _, v := range list {
				if v == verb {
					contains = true
					break
				}
			}

			tok.ForVerb(verb)

			require.Equal(t, contains, assertVerb(tok, op),
				"%v in token, %s executing", verb, op)
		}
	}
}

func TestAssertSessionRelation(t *testing.T) {
	var tok sessionSDK.Object
	cnr := cidtest.ID()
	cnrOther := cidtest.ID()
	obj := oidtest.ID()
	objOther := oidtest.ID()

	// make sure ids differ, otherwise test won't work correctly
	require.False(t, cnrOther == cnr)
	require.False(t, objOther == obj)

	// bind session to the container (required)
	tok.BindContainer(cnr)

	// test container-global session
	require.NoError(t, assertSessionRelation(tok, cnr, nil))
	require.NoError(t, assertSessionRelation(tok, cnr, &obj))
	require.Error(t, assertSessionRelation(tok, cnrOther, nil))
	require.Error(t, assertSessionRelation(tok, cnrOther, &obj))

	// limit the session to the particular object
	tok.LimitByObjects(obj)

	// test fixed object session (here obj arg must be non-nil everywhere)
	require.NoError(t, assertSessionRelation(tok, cnr, &obj))
	require.Error(t, assertSessionRelation(tok, cnr, &objOther))
}
