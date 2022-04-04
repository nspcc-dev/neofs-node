package object

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	sessiontest "github.com/nspcc-dev/neofs-sdk-go/session/test"
	"github.com/nspcc-dev/neofs-sdk-go/storagegroup"
	"github.com/stretchr/testify/require"
)

func testSHA(t *testing.T) [sha256.Size]byte {
	cs := [sha256.Size]byte{}

	_, err := rand.Read(cs[:])
	require.NoError(t, err)

	return cs
}

func testObjectID(t *testing.T) *oidSDK.ID {
	id := oidSDK.NewID()
	id.SetSHA256(testSHA(t))

	return id
}

func blankValidObject(key *ecdsa.PrivateKey) *object.Object {
	obj := object.New()
	obj.SetContainerID(cidtest.ID())
	obj.SetOwnerID(owner.NewIDFromPublicKey(&key.PublicKey))

	return obj
}

type testNetState struct {
	epoch uint64
}

func (s testNetState) CurrentEpoch() uint64 {
	return s.epoch
}

func TestFormatValidator_Validate(t *testing.T) {
	const curEpoch = 13

	v := NewFormatValidator(
		WithNetState(testNetState{
			epoch: curEpoch,
		}),
	)

	ownerKey, err := keys.NewPrivateKey()
	require.NoError(t, err)

	t.Run("nil input", func(t *testing.T) {
		require.Error(t, v.Validate(nil, true))
	})

	t.Run("nil identifier", func(t *testing.T) {
		obj := object.New()

		require.True(t, errors.Is(v.Validate(obj, false), errNilID))
	})

	t.Run("nil container identifier", func(t *testing.T) {
		obj := object.New()
		obj.SetID(testObjectID(t))

		require.True(t, errors.Is(v.Validate(obj, true), errNilCID))
	})

	t.Run("unsigned object", func(t *testing.T) {
		obj := object.New()
		obj.SetContainerID(cidtest.ID())
		obj.SetID(testObjectID(t))

		require.Error(t, v.Validate(obj, true))
	})

	t.Run("correct w/ session token", func(t *testing.T) {
		oid := owner.NewIDFromPublicKey((*ecdsa.PublicKey)(ownerKey.PublicKey()))

		tok := sessiontest.Token()
		tok.SetOwnerID(oid)

		obj := object.New()
		obj.SetContainerID(cidtest.ID())
		obj.SetSessionToken(sessiontest.Token())
		obj.SetOwnerID(tok.OwnerID())

		require.NoError(t, object.SetIDWithSignature(&ownerKey.PrivateKey, obj))

		require.NoError(t, v.Validate(obj, false))
	})

	t.Run("correct w/o session token", func(t *testing.T) {
		obj := blankValidObject(&ownerKey.PrivateKey)

		require.NoError(t, object.SetIDWithSignature(&ownerKey.PrivateKey, obj))

		require.NoError(t, v.Validate(obj, false))
	})

	t.Run("tombstone content", func(t *testing.T) {
		obj := object.New()
		obj.SetType(object.TypeTombstone)

		require.Error(t, v.ValidateContent(obj)) // no tombstone content

		content := object.NewTombstone()
		content.SetMembers([]oidSDK.ID{*testObjectID(t)})

		data, err := content.Marshal()
		require.NoError(t, err)

		obj.SetPayload(data)

		require.Error(t, v.ValidateContent(obj)) // no members in tombstone

		content.SetMembers([]oidSDK.ID{*testObjectID(t)})

		data, err = content.Marshal()
		require.NoError(t, err)

		obj.SetPayload(data)

		require.Error(t, v.ValidateContent(obj)) // no expiration epoch in tombstone

		var expirationAttribute object.Attribute
		expirationAttribute.SetKey(objectV2.SysAttributeExpEpoch)
		expirationAttribute.SetValue(strconv.Itoa(10))

		obj.SetAttributes(expirationAttribute)

		require.Error(t, v.ValidateContent(obj)) // different expiration values

		content.SetExpirationEpoch(10)
		data, err = content.Marshal()
		require.NoError(t, err)

		obj.SetPayload(data)

		require.NoError(t, v.ValidateContent(obj)) // all good
	})

	t.Run("storage group content", func(t *testing.T) {
		obj := object.New()
		obj.SetType(object.TypeStorageGroup)

		require.Error(t, v.ValidateContent(obj))

		content := storagegroup.New()
		content.SetMembers([]oidSDK.ID{})

		data, err := content.Marshal()
		require.NoError(t, err)

		obj.SetPayload(data)

		require.Error(t, v.ValidateContent(obj))

		content.SetMembers([]oidSDK.ID{*testObjectID(t)})

		data, err = content.Marshal()
		require.NoError(t, err)

		obj.SetPayload(data)

		require.NoError(t, v.ValidateContent(obj))
	})

	t.Run("expiration", func(t *testing.T) {
		fn := func(val string) *object.Object {
			obj := blankValidObject(&ownerKey.PrivateKey)

			var a object.Attribute
			a.SetKey(objectV2.SysAttributeExpEpoch)
			a.SetValue(val)

			obj.SetAttributes(a)

			require.NoError(t, object.SetIDWithSignature(&ownerKey.PrivateKey, obj))

			return obj
		}

		t.Run("invalid attribute value", func(t *testing.T) {
			val := "text"
			err := v.Validate(fn(val), false)
			require.Error(t, err)
		})

		t.Run("expired object", func(t *testing.T) {
			val := strconv.FormatUint(curEpoch-1, 10)
			err := v.Validate(fn(val), false)
			require.True(t, errors.Is(err, errExpired))
		})

		t.Run("alive object", func(t *testing.T) {
			val := strconv.FormatUint(curEpoch, 10)
			err := v.Validate(fn(val), true)
			require.NoError(t, err)
		})
	})

	t.Run("attributes", func(t *testing.T) {
		t.Run("duplication", func(t *testing.T) {
			obj := blankValidObject(&ownerKey.PrivateKey)

			var a1 object.Attribute
			a1.SetKey("key1")
			a1.SetValue("val1")

			var a2 object.Attribute
			a2.SetKey("key2")
			a2.SetValue("val2")

			obj.SetAttributes(a1, a2)

			err := v.checkAttributes(obj)
			require.NoError(t, err)

			a2.SetKey(a1.Key())
			obj.SetAttributes(a1, a2)

			err = v.checkAttributes(obj)
			require.Equal(t, errDuplAttr, err)
		})

		t.Run("empty value", func(t *testing.T) {
			obj := blankValidObject(&ownerKey.PrivateKey)

			var a object.Attribute
			a.SetKey("key")

			obj.SetAttributes(a)

			err := v.checkAttributes(obj)
			require.Equal(t, errEmptyAttrVal, err)
		})
	})
}
