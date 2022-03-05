package v2

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	acltest "github.com/nspcc-dev/neofs-api-go/v2/acl/test"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	sessiontest "github.com/nspcc-dev/neofs-api-go/v2/session/test"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	bearerSDK "github.com/nspcc-dev/neofs-sdk-go/token"
	"github.com/stretchr/testify/require"
)

func TestOriginalTokens(t *testing.T) {
	sToken := sessiontest.GenerateSessionToken(false)
	bToken := acltest.GenerateBearerToken(false)

	for i := 0; i < 10; i++ {
		metaHeaders := testGenerateMetaHeader(uint32(i), bToken, sToken)
		require.Equal(t, sessionSDK.NewTokenFromV2(sToken), originalSessionToken(metaHeaders), i)
		require.Equal(t, bearerSDK.NewBearerTokenFromV2(bToken), originalBearerToken(metaHeaders), i)
	}
}

func testGenerateMetaHeader(depth uint32, b *acl.BearerToken, s *session.SessionToken) *session.RequestMetaHeader {
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
