package v2

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	acltest "github.com/nspcc-dev/neofs-api-go/v2/acl/test"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	sessiontest "github.com/nspcc-dev/neofs-sdk-go/session/test"
	"github.com/stretchr/testify/require"
)

func TestOriginalTokens(t *testing.T) {
	sToken := sessiontest.ObjectSigned()
	bTokenV2 := acltest.GenerateBearerToken(false)

	var bToken bearer.Token
	bToken.ReadFromV2(*bTokenV2)

	var sTokenV2 session.Token
	sToken.WriteToV2(&sTokenV2)

	for i := 0; i < 10; i++ {
		metaHeaders := testGenerateMetaHeader(uint32(i), bTokenV2, &sTokenV2)
		res, err := originalSessionToken(metaHeaders)
		require.NoError(t, err)
		require.Equal(t, sToken, res, i)
		require.Equal(t, &bToken, originalBearerToken(metaHeaders), i)
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
