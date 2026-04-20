package acl

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	v2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

type emptyEACLSource struct{}

func (e emptyEACLSource) GetEACL(_ cid.ID) (eaclSDK.Table, error) {
	return eaclSDK.Table{}, nil
}

type emptyHeaderSource struct{}

func (e emptyHeaderSource) Head(address oid.Address) (*object.Object, error) {
	return nil, nil
}

func TestStickyCheck(t *testing.T) {
	checker := NewChecker(new(CheckerPrm).
		SetLocalStorage(&engine.StorageEngine{}).
		SetValidator(eaclSDK.NewValidator()).
		SetEACLSource(emptyEACLSource{}).SetHeaderSource(emptyHeaderSource{}))

	t.Run("system role", func(t *testing.T) {
		var info = v2.RequestInfo{
			RequestRole: acl.RoleContainer,
			SenderKey:   make([]byte, 33), // any non-empty key
		}

		require.True(t, checker.StickyBitCheck(info, usertest.ID()))

		var basicACL acl.Basic
		basicACL.MakeSticky()

		info.BasicACL = basicACL

		require.True(t, checker.StickyBitCheck(info, usertest.ID()))
	})

	t.Run("owner ID and/or public key emptiness", func(t *testing.T) {
		var info = v2.RequestInfo{
			RequestRole: acl.RoleOthers, // should be non-system role
		}

		assertFn := func(isSticky, withKey, withOwner, expected bool) {
			info := info
			if isSticky {
				var basicACL acl.Basic
				basicACL.MakeSticky()

				info.BasicACL = basicACL
			}

			if withKey {
				info.SenderKey = make([]byte, 33)
			} else {
				info.SenderKey = nil
			}

			var ownerID user.ID

			if withOwner {
				ownerID = usertest.ID()
			}

			require.Equal(t, expected, checker.StickyBitCheck(info, ownerID))
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
