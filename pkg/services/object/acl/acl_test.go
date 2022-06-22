package acl

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	v2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

type emptyEACLSource struct{}

func (e emptyEACLSource) GetEACL(_ cid.ID) (*container.EACL, error) {
	return nil, nil
}

type emptyNetmapState struct{}

func (e emptyNetmapState) CurrentEpoch() uint64 {
	return 0
}

func TestStickyCheck(t *testing.T) {
	checker := NewChecker(new(CheckerPrm).
		SetLocalStorage(&engine.StorageEngine{}).
		SetValidator(eaclSDK.NewValidator()).
		SetEACLSource(emptyEACLSource{}).
		SetNetmapState(emptyNetmapState{}),
	)

	t.Run("system role", func(t *testing.T) {
		var info v2.RequestInfo

		info.SetSenderKey(make([]byte, 33)) // any non-empty key
		info.SetRequestRole(eaclSDK.RoleSystem)

		setSticky(&info, true)

		require.True(t, checker.StickyBitCheck(info, *usertest.ID()))

		setSticky(&info, false)

		require.True(t, checker.StickyBitCheck(info, *usertest.ID()))
	})

	t.Run("owner ID and/or public key emptiness", func(t *testing.T) {
		var info v2.RequestInfo

		info.SetRequestRole(eaclSDK.RoleOthers) // should be non-system role

		assertFn := func(isSticky, withKey, withOwner, expected bool) {
			if isSticky {
				setSticky(&info, true)
			} else {
				setSticky(&info, false)
			}

			if withKey {
				info.SetSenderKey(make([]byte, 33))
			} else {
				info.SetSenderKey(nil)
			}

			var ownerID user.ID

			if withOwner {
				ownerID = *usertest.ID()
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

func setSticky(req *v2.RequestInfo, enabled bool) {
	bh := basicACLHelper(req.BasicACL())

	if enabled {
		bh.SetSticky()
	} else {
		bh.ResetSticky()
	}

	req.SetBasicACL(uint32(bh))
}
