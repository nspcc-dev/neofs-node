package object

import (
	"context"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	sessiontest "github.com/nspcc-dev/neofs-sdk-go/session/test"
	"github.com/nspcc-dev/neofs-sdk-go/storagegroup"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/stretchr/testify/require"
)

func blankValidObject(signer user.Signer) *object.Object {
	idOwner := signer.UserID()

	obj := object.New()
	obj.SetContainerID(cidtest.ID())
	obj.SetOwnerID(&idOwner)

	return obj
}

type testNetState struct {
	epoch uint64
}

func (s testNetState) CurrentEpoch() uint64 {
	return s.epoch
}

type testLockSource struct {
	m map[oid.Address]bool
}

func (t testLockSource) IsLocked(address oid.Address) (bool, error) {
	return t.m[address], nil
}

type tombstoneVerifier struct {
}

func (t2 tombstoneVerifier) VerifyTomb(_ context.Context, _ cid.ID, _ object.Tombstone) error {
	return nil
}

func TestFormatValidator_Validate(t *testing.T) {
	const curEpoch = 13

	ls := testLockSource{
		m: make(map[oid.Address]bool),
	}

	v := NewFormatValidator(
		WithNetState(testNetState{
			epoch: curEpoch,
		}),
		WithLockSource(ls),
		WithTombVerifier(tombstoneVerifier{}),
	)

	ownerKey, err := keys.NewPrivateKey()
	require.NoError(t, err)

	t.Run("nil input", func(t *testing.T) {
		require.Error(t, v.Validate(nil, true))
	})

	t.Run("nil identifier", func(t *testing.T) {
		obj := object.New()

		require.ErrorIs(t, v.Validate(obj, false), errNilID)
	})

	t.Run("nil container identifier", func(t *testing.T) {
		obj := object.New()
		obj.SetID(oidtest.ID())

		require.ErrorIs(t, v.Validate(obj, true), errNilCID)
	})

	t.Run("unsigned object", func(t *testing.T) {
		obj := object.New()
		obj.SetContainerID(cidtest.ID())
		obj.SetID(oidtest.ID())

		require.Error(t, v.Validate(obj, false))
	})

	t.Run("correct w/ session token", func(t *testing.T) {
		signer := user.NewAutoIDSignerRFC6979(ownerKey.PrivateKey)

		obj := object.New()
		obj.SetContainerID(cidtest.ID())
		tok := sessiontest.Object()
		tok.SetAuthKey((*neofsecdsa.PublicKey)(&ownerKey.PrivateKey.PublicKey))
		require.NoError(t, tok.Sign(signer))
		obj.SetSessionToken(&tok)
		owner := signer.UserID()
		obj.SetOwnerID(&owner)

		require.NoError(t, obj.SetIDWithSignature(signer))

		require.NoError(t, v.Validate(obj, false))
	})

	t.Run("incorrect session token", func(t *testing.T) {
		signer := user.NewAutoIDSignerRFC6979(ownerKey.PrivateKey)

		obj := object.New()
		obj.SetContainerID(cidtest.ID())
		tok := sessiontest.ObjectSigned(signer)
		obj.SetSessionToken(&tok)
		owner := signer.UserID()
		obj.SetOwnerID(&owner)

		t.Run("wrong signature", func(t *testing.T) {
			require.NoError(t, obj.SetIDWithSignature(signer))

			obj.SetSignature(&neofscrypto.Signature{})
			require.Error(t, v.Validate(obj, false))
		})

		t.Run("wrong owner", func(t *testing.T) {
			obj.SetOwnerID(&user.ID{})

			require.NoError(t, obj.SetIDWithSignature(signer))
			require.Error(t, v.Validate(obj, false))
		})

		t.Run("wrong signer", func(t *testing.T) {
			wrongOwner, err := keys.NewPrivateKey()
			require.NoError(t, err)

			wrongSigner := user.NewAutoIDSignerRFC6979(wrongOwner.PrivateKey)

			require.NoError(t, obj.SetIDWithSignature(wrongSigner))
			require.Error(t, v.Validate(obj, false))
		})
	})

	t.Run("correct w/o session token", func(t *testing.T) {
		signer := user.NewAutoIDSigner(ownerKey.PrivateKey)
		obj := blankValidObject(signer)

		require.NoError(t, obj.SetIDWithSignature(signer))

		require.NoError(t, v.Validate(obj, false))
	})

	t.Run("tombstone content", func(t *testing.T) {
		obj := object.New()
		obj.SetType(object.TypeTombstone)
		obj.SetContainerID(cidtest.ID())

		_, err := v.ValidateContent(obj)
		require.Error(t, err) // no tombstone content

		content := object.NewTombstone()
		content.SetMembers([]oid.ID{oidtest.ID()})

		obj.SetPayload(content.Marshal())

		_, err = v.ValidateContent(obj)
		require.Error(t, err) // no members in tombstone

		content.SetMembers([]oid.ID{oidtest.ID()})

		obj.SetPayload(content.Marshal())

		_, err = v.ValidateContent(obj)
		require.Error(t, err) // no expiration epoch in tombstone

		var expirationAttribute object.Attribute
		expirationAttribute.SetKey(object.AttributeExpirationEpoch)
		expirationAttribute.SetValue(strconv.Itoa(10))

		obj.SetAttributes(expirationAttribute)

		_, err = v.ValidateContent(obj)
		require.Error(t, err) // different expiration values

		id := oidtest.ID()

		content.SetExpirationEpoch(10)
		content.SetMembers([]oid.ID{id})

		obj.SetPayload(content.Marshal())

		contentGot, err := v.ValidateContent(obj)
		require.NoError(t, err) // all good

		require.EqualValues(t, []oid.ID{id}, contentGot.Objects())
		require.Equal(t, object.TypeTombstone, contentGot.Type())
	})

	t.Run("storage group content", func(t *testing.T) {
		obj := object.New()
		obj.SetType(object.TypeStorageGroup)

		t.Run("empty payload", func(t *testing.T) {
			_, err := v.ValidateContent(obj)
			require.Error(t, err)
		})

		var content storagegroup.StorageGroup
		content.SetValidationDataSize(1) // some non-default value

		t.Run("empty members", func(t *testing.T) {
			obj.SetPayload(content.Marshal())

			_, err = v.ValidateContent(obj)
			require.ErrorIs(t, err, errEmptySGMembers)
		})

		t.Run("non-unique members", func(t *testing.T) {
			id := oidtest.ID()

			content.SetMembers([]oid.ID{id, id})

			obj.SetPayload(content.Marshal())

			_, err = v.ValidateContent(obj)
			require.Error(t, err)
		})

		t.Run("correct SG", func(t *testing.T) {
			ids := []oid.ID{oidtest.ID(), oidtest.ID()}
			content.SetMembers(ids)

			obj.SetPayload(content.Marshal())

			content, err := v.ValidateContent(obj)
			require.NoError(t, err)

			require.EqualValues(t, ids, content.Objects())
			require.Equal(t, object.TypeStorageGroup, content.Type())
		})
	})

	t.Run("expiration", func(t *testing.T) {
		fn := func(val string) *object.Object {
			signer := user.NewAutoIDSigner(ownerKey.PrivateKey)
			obj := blankValidObject(signer)

			var a object.Attribute
			a.SetKey(object.AttributeExpirationEpoch)
			a.SetValue(val)

			obj.SetAttributes(a)

			require.NoError(t, obj.SetIDWithSignature(signer))

			return obj
		}

		t.Run("invalid attribute value", func(t *testing.T) {
			val := "text"
			err := v.Validate(fn(val), false)
			require.Error(t, err)
		})

		t.Run("expired object", func(t *testing.T) {
			val := strconv.FormatUint(curEpoch-1, 10)
			obj := fn(val)

			t.Run("non-locked", func(t *testing.T) {
				err := v.Validate(obj, false)
				require.ErrorIs(t, err, errExpired)
			})

			t.Run("locked", func(t *testing.T) {
				var addr oid.Address
				oID := obj.GetID()
				cID := obj.GetContainerID()

				addr.SetContainer(cID)
				addr.SetObject(oID)
				ls.m[addr] = true

				err := v.Validate(obj, false)
				require.NoError(t, err)
			})
		})

		t.Run("alive object", func(t *testing.T) {
			val := strconv.FormatUint(curEpoch, 10)
			err := v.Validate(fn(val), true)
			require.NoError(t, err)
		})
	})

	t.Run("attributes", func(t *testing.T) {
		t.Run("duplication", func(t *testing.T) {
			signer := user.NewAutoIDSigner(ownerKey.PrivateKey)
			obj := blankValidObject(signer)

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
			signer := user.NewAutoIDSigner(ownerKey.PrivateKey)
			obj := blankValidObject(signer)

			var a object.Attribute
			a.SetKey("key")

			obj.SetAttributes(a)

			err := v.checkAttributes(obj)
			require.Equal(t, errEmptyAttrVal, err)
		})
	})
}

type testSplitVerifier struct {
}

func (t *testSplitVerifier) VerifySplit(ctx context.Context, cID cid.ID, firstID oid.ID, children []object.MeasuredObject) error {
	return nil
}

func TestLinkObjectSplitV2(t *testing.T) {
	verifier := new(testSplitVerifier)

	v := NewFormatValidator(
		WithSplitVerifier(verifier),
	)

	ownerKey, err := keys.NewPrivateKey()
	require.NoError(t, err)

	signer := user.NewAutoIDSigner(ownerKey.PrivateKey)
	obj := blankValidObject(signer)
	obj.SetParent(object.New())
	obj.SetSplitID(object.NewSplitID())
	obj.SetFirstID(oidtest.ID())

	t.Run("V1 split, first is set", func(t *testing.T) {
		require.ErrorContains(t, v.Validate(obj, true), "first object ID is set")
	})

	t.Run("V2 split", func(t *testing.T) {
		t.Run("link object without finished parent", func(t *testing.T) {
			obj.ResetRelations()
			obj.SetParent(object.New())
			obj.SetFirstID(oidtest.ID())
			obj.SetType(object.TypeLink)

			require.ErrorContains(t, v.Validate(obj, true), "incorrect link object's parent header")
		})

		t.Run("middle child does not have previous object ID", func(t *testing.T) {
			obj.ResetRelations()
			obj.SetParent(object.New())
			obj.SetFirstID(oidtest.ID())
			obj.SetType(object.TypeRegular)

			require.ErrorContains(t, v.Validate(obj, true), "middle part does not have previous object ID")
		})
	})
}
