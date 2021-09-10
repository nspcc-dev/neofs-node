package acl

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	ownertest "github.com/nspcc-dev/neofs-api-go/pkg/owner/test"
	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	acltest "github.com/nspcc-dev/neofs-api-go/v2/acl/test"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	sessiontest "github.com/nspcc-dev/neofs-api-go/v2/session/test"
	"github.com/stretchr/testify/require"
)

func TestOriginalTokens(t *testing.T) {
	sToken := sessiontest.GenerateSessionToken(false)
	bToken := acltest.GenerateBearerToken(false)

	for i := 0; i < 10; i++ {
		metaHeaders := testGenerateMetaHeader(uint32(i), bToken, sToken)
		require.Equal(t, sToken, originalSessionToken(metaHeaders), i)
		require.Equal(t, bToken, originalBearerToken(metaHeaders), i)
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

func TestStickyCheck(t *testing.T) {
	t.Run("system role", func(t *testing.T) {
		var info requestInfo

		info.senderKey = make([]byte, 33) // any non-empty key
		info.requestRole = eacl.RoleSystem

		info.basicACL.SetSticky()
		require.True(t, stickyBitCheck(info, ownertest.Generate()))

		info.basicACL.ResetSticky()
		require.True(t, stickyBitCheck(info, ownertest.Generate()))
	})
}
