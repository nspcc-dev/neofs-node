package v2

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	bearertest "github.com/nspcc-dev/neofs-sdk-go/bearer/test"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	sessiontest "github.com/nspcc-dev/neofs-sdk-go/session/test"
	"github.com/stretchr/testify/require"
)

func TestOriginalTokens(t *testing.T) {
	sToken := sessiontest.ObjectSigned()
	bToken := bearertest.Token()

	pk, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, bToken.Sign(*pk))

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
		require.Equal(t, sToken, res, i)

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
	table := map[eacl.Operation][]sessionSDK.ObjectVerb{
		eacl.OperationPut:    {sessionSDK.VerbObjectPut, sessionSDK.VerbObjectDelete},
		eacl.OperationDelete: {sessionSDK.VerbObjectDelete},
		eacl.OperationGet:    {sessionSDK.VerbObjectGet},
		eacl.OperationHead: {
			sessionSDK.VerbObjectHead,
			sessionSDK.VerbObjectGet,
			sessionSDK.VerbObjectDelete,
			sessionSDK.VerbObjectRange,
			sessionSDK.VerbObjectRangeHash,
		},
		eacl.OperationRange:     {sessionSDK.VerbObjectRange, sessionSDK.VerbObjectRangeHash},
		eacl.OperationRangeHash: {sessionSDK.VerbObjectRangeHash},
		eacl.OperationSearch:    {sessionSDK.VerbObjectSearch, sessionSDK.VerbObjectDelete},
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
