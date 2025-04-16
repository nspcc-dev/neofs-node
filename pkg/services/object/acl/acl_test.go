package acl

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	v2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

type emptyEACLSource struct{}

func (e emptyEACLSource) GetEACL(_ cid.ID) (*container.EACL, error) {
	return nil, nil
}

type emptyNetmapState struct{}

type emptyHeaderSource struct{}

func (e emptyHeaderSource) Head(address oid.Address) (*object.Object, error) {
	return nil, nil
}

func (e emptyNetmapState) CurrentEpoch() uint64 {
	return 0
}

func TestStickyCheck(t *testing.T) {
	checker := NewChecker(new(CheckerPrm).
		SetLocalStorage(&engine.StorageEngine{}).
		SetValidator(eaclSDK.NewValidator()).
		SetEACLSource(emptyEACLSource{}).
		SetNetmapState(emptyNetmapState{}).
		SetHeaderSource(emptyHeaderSource{}),
	)

	t.Run("system role", func(t *testing.T) {
		var info v2.RequestInfo

		info.SetSenderKey(make([]byte, 33)) // any non-empty key
		info.SetRequestRole(acl.RoleContainer)

		require.True(t, checker.StickyBitCheck(info, usertest.ID()))

		var basicACL acl.Basic
		basicACL.MakeSticky()

		info.SetBasicACL(basicACL)

		require.True(t, checker.StickyBitCheck(info, usertest.ID()))
	})

	t.Run("owner ID and/or public key emptiness", func(t *testing.T) {
		var info v2.RequestInfo

		info.SetRequestRole(acl.RoleOthers) // should be non-system role

		assertFn := func(isSticky, withKey, withOwner, expected bool) {
			info := info
			if isSticky {
				var basicACL acl.Basic
				basicACL.MakeSticky()

				info.SetBasicACL(basicACL)
			}

			if withKey {
				info.SetSenderKey(make([]byte, 33))
			} else {
				info.SetSenderKey(nil)
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

func TestIsValidBearer(t *testing.T) {
	t.Run("signed not by issuer", func(t *testing.T) {
		const curEpoch = 100
		brr := usertest.User()
		cnrOwner := usertest.User()
		var bt bearer.Token
		bt.SetExp(curEpoch)
		bt.SetIssuer(usertest.OtherID(cnrOwner.ID))

		sig, err := cnrOwner.Sign(bt.SignedData())
		require.NoError(t, err)
		bt.AttachSignature(neofscrypto.NewSignatureFromRawKey(cnrOwner.Signer.Scheme(), cnrOwner.PublicKeyBytes, sig))

		c := &Checker{
			state: emptyNetmapState{},
		}

		err = c.isValidBearer(bt, cidtest.ID(), cnrOwner.ID, brr.ID)
		require.EqualError(t, err, "authenticate bearer token: issuer mismatches signature")
	})
}
