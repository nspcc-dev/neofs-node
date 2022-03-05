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

func testObjectID(t *testing.T) *object.ID {
	id := object.NewID()
	id.SetSHA256(testSHA(t))

	return id
}

func blankValidObject(key *ecdsa.PrivateKey) *RawObject {
	obj := NewRaw()
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
		require.Error(t, v.Validate(nil))
	})

	t.Run("nil identifier", func(t *testing.T) {
		obj := NewRaw()

		require.True(t, errors.Is(v.Validate(obj.Object()), errNilID))
	})

	t.Run("nil container identifier", func(t *testing.T) {
		obj := NewRaw()
		obj.SetID(testObjectID(t))

		require.True(t, errors.Is(v.Validate(obj.Object()), errNilCID))
	})

	t.Run("unsigned object", func(t *testing.T) {
		obj := NewRaw()
		obj.SetContainerID(cidtest.ID())
		obj.SetID(testObjectID(t))

		require.Error(t, v.Validate(obj.Object()))
	})

	t.Run("correct w/ session token", func(t *testing.T) {
		oid := owner.NewIDFromPublicKey((*ecdsa.PublicKey)(ownerKey.PublicKey()))

		tok := sessiontest.Token()
		tok.SetOwnerID(oid)

		obj := NewRaw()
		obj.SetContainerID(cidtest.ID())
		obj.SetSessionToken(sessiontest.Token())
		obj.SetOwnerID(tok.OwnerID())

		require.NoError(t, object.SetIDWithSignature(&ownerKey.PrivateKey, obj.SDK()))

		require.NoError(t, v.Validate(obj.Object()))
	})

	t.Run("correct w/o session token", func(t *testing.T) {
		obj := blankValidObject(&ownerKey.PrivateKey)

		require.NoError(t, object.SetIDWithSignature(&ownerKey.PrivateKey, obj.SDK()))

		require.NoError(t, v.Validate(obj.Object()))
	})

	t.Run("tombstone content", func(t *testing.T) {
		obj := NewRaw()
		obj.SetType(object.TypeTombstone)

		require.Error(t, v.ValidateContent(obj.Object())) // no tombstone content

		content := object.NewTombstone()
		content.SetMembers([]*object.ID{nil})

		data, err := content.Marshal()
		require.NoError(t, err)

		obj.SetPayload(data)

		require.Error(t, v.ValidateContent(obj.Object())) // no members in tombstone

		content.SetMembers([]*object.ID{testObjectID(t)})

		data, err = content.Marshal()
		require.NoError(t, err)

		obj.SetPayload(data)

		require.Error(t, v.ValidateContent(obj.Object())) // no expiration epoch in tombstone

		expirationAttribute := object.NewAttribute()
		expirationAttribute.SetKey(objectV2.SysAttributeExpEpoch)
		expirationAttribute.SetValue(strconv.Itoa(10))

		obj.SetAttributes(expirationAttribute)

		require.Error(t, v.ValidateContent(obj.Object())) // different expiration values

		content.SetExpirationEpoch(10)
		data, err = content.Marshal()
		require.NoError(t, err)

		obj.SetPayload(data)

		require.NoError(t, v.ValidateContent(obj.Object())) // all good
	})

	t.Run("storage group content", func(t *testing.T) {
		obj := NewRaw()
		obj.SetType(object.TypeStorageGroup)

		require.Error(t, v.ValidateContent(obj.Object()))

		content := storagegroup.New()
		content.SetMembers([]*object.ID{nil})

		data, err := content.Marshal()
		require.NoError(t, err)

		obj.SetPayload(data)

		require.Error(t, v.ValidateContent(obj.Object()))

		content.SetMembers([]*object.ID{testObjectID(t)})

		data, err = content.Marshal()
		require.NoError(t, err)

		obj.SetPayload(data)

		require.NoError(t, v.ValidateContent(obj.Object()))
	})

	t.Run("expiration", func(t *testing.T) {
		fn := func(val string) *Object {
			obj := blankValidObject(&ownerKey.PrivateKey)

			a := object.NewAttribute()
			a.SetKey(objectV2.SysAttributeExpEpoch)
			a.SetValue(val)

			obj.SetAttributes(a)

			require.NoError(t, object.SetIDWithSignature(&ownerKey.PrivateKey, obj.SDK()))

			return obj.Object()
		}

		t.Run("invalid attribute value", func(t *testing.T) {
			val := "text"
			err := v.Validate(fn(val))
			require.Error(t, err)
		})

		t.Run("expired object", func(t *testing.T) {
			val := strconv.FormatUint(curEpoch-1, 10)
			err := v.Validate(fn(val))
			require.True(t, errors.Is(err, errExpired))
		})

		t.Run("alive object", func(t *testing.T) {
			val := strconv.FormatUint(curEpoch, 10)
			err := v.Validate(fn(val))
			require.NoError(t, err)
		})
	})

	t.Run("attributes", func(t *testing.T) {
		t.Run("duplication", func(t *testing.T) {
			obj := blankValidObject(&ownerKey.PrivateKey)

			a1 := object.NewAttribute()
			a1.SetKey("key1")
			a1.SetValue("val1")

			a2 := object.NewAttribute()
			a2.SetKey("key2")
			a2.SetValue("val2")

			obj.SetAttributes(a1, a2)

			err := v.checkAttributes(obj.Object())
			require.NoError(t, err)

			a2.SetKey(a1.Key())

			err = v.checkAttributes(obj.Object())
			require.Equal(t, errDuplAttr, err)
		})

		t.Run("empty value", func(t *testing.T) {
			obj := blankValidObject(&ownerKey.PrivateKey)

			a := object.NewAttribute()
			a.SetKey("key")

			obj.SetAttributes(a)

			err := v.checkAttributes(obj.Object())
			require.Equal(t, errEmptyAttrVal, err)
		})
	})
}
