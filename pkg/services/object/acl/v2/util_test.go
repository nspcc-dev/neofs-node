package v2

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	bearertest "github.com/nspcc-dev/neofs-sdk-go/bearer/test"
	aclsdk "github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	sessiontest "github.com/nspcc-dev/neofs-sdk-go/session/test"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/stretchr/testify/require"
)

func TestOriginalTokens(t *testing.T) {
	pk, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	signer := user.NewAutoIDSigner(*pk)

	sToken := sessiontest.ObjectSigned(signer)
	bToken := bearertest.Token(t)

	require.NoError(t, bToken.Sign(signer))

	var bTokenV2 acl.BearerToken
	bToken.WriteToV2(&bTokenV2)
	// This line is needed because SDK uses some custom format for
	// reserved filters, so `cid.ID` is not converted to string immediately.
	require.NoError(t, bToken.ReadFromV2(bTokenV2))

	var sTokenV2 session.Token
	sToken.WriteToV2(&sTokenV2)

	for i := 0; i < 10; i++ {
		metaHeaders := testGenerateMetaHeader(uint32(i), &bTokenV2, &sTokenV2)
		res, err := originalSessionToken(metaHeaders)
		require.NoError(t, err)
		require.Equal(t, sToken, *res, i)

		bTok, err := originalBearerToken(metaHeaders)
		require.NoError(t, err)
		require.Equal(t, &bToken, bTok, i)
	}
}

func testGenerateMetaHeader(depth uint32, b *acl.BearerToken, s *session.Token) *session.RequestMetaHeader {
	metaHeader := new(session.RequestMetaHeader)
	metaHeader.SetBearerToken(b)
	metaHeader.SetSessionToken(s)

	for i := uint32(0); i < depth; i++ {
		link := metaHeader
		metaHeader = new(session.RequestMetaHeader)
		metaHeader.SetOrigin(link)
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
	require.False(t, cnrOther.Equals(cnr))
	require.False(t, objOther.Equals(obj))

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
