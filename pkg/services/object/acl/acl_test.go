package acl

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	acltest "github.com/nspcc-dev/neofs-api-go/v2/acl/test"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	sessiontest "github.com/nspcc-dev/neofs-api-go/v2/session/test"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	ownertest "github.com/nspcc-dev/neofs-sdk-go/owner/test"
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
		require.True(t, stickyBitCheck(info, ownertest.GenerateID()))

		info.basicACL.ResetSticky()
		require.True(t, stickyBitCheck(info, ownertest.GenerateID()))
	})

	t.Run("owner ID and/or public key emptiness", func(t *testing.T) {
		var info requestInfo

		info.requestRole = eacl.RoleOthers // should be non-system role

		assertFn := func(isSticky, withKey, withOwner, expected bool) {
			if isSticky {
				info.basicACL.SetSticky()
			} else {
				info.basicACL.ResetSticky()
			}

			if withKey {
				info.senderKey = make([]byte, 33)
			} else {
				info.senderKey = nil
			}

			var ownerID *owner.ID

			if withOwner {
				ownerID = ownertest.GenerateID()
			}

			require.Equal(t, expected, stickyBitCheck(info, ownerID))
		}

		assertFn(true, false, false, false)
		assertFn(true, true, false, false)
		assertFn(true, false, true, false)
		assertFn(false, false, false, true)
		assertFn(false, true, false, true)
		assertFn(false, false, true, true)
		assertFn(false, true, true, true)
	})
}
