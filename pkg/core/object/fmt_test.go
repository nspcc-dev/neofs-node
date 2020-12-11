package object

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func testSHA(t *testing.T) [sha256.Size]byte {
	cs := [sha256.Size]byte{}

	_, err := rand.Read(cs[:])
	require.NoError(t, err)

	return cs
}

func testContainerID(t *testing.T) *container.ID {
	id := container.NewID()
	id.SetSHA256(testSHA(t))

	return id
}

func testObjectID(t *testing.T) *object.ID {
	id := object.NewID()
	id.SetSHA256(testSHA(t))

	return id
}

func blankValidObject(t *testing.T, key *ecdsa.PrivateKey) *RawObject {
	wallet, err := owner.NEO3WalletFromPublicKey(&key.PublicKey)
	require.NoError(t, err)

	ownerID := owner.NewID()
	ownerID.SetNeo3Wallet(wallet)

	obj := NewRaw()
	obj.SetContainerID(testContainerID(t))
	obj.SetOwnerID(ownerID)

	return obj
}

func TestFormatValidator_Validate(t *testing.T) {
	v := NewFormatValidator()

	ownerKey := test.DecodeKey(-1)

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
		obj.SetContainerID(testContainerID(t))
		obj.SetID(testObjectID(t))

		require.Error(t, v.Validate(obj.Object()))
	})

	t.Run("correct w/ session token", func(t *testing.T) {
		tok := token.NewSessionToken()
		tok.SetSessionKey(crypto.MarshalPublicKey(&ownerKey.PublicKey))

		obj := NewRaw()
		obj.SetContainerID(testContainerID(t))
		obj.SetSessionToken(tok)

		require.NoError(t, object.SetIDWithSignature(ownerKey, obj.SDK()))

		require.NoError(t, v.Validate(obj.Object()))
	})

	t.Run("correct w/o session token", func(t *testing.T) {
		obj := blankValidObject(t, ownerKey)

		require.NoError(t, object.SetIDWithSignature(ownerKey, obj.SDK()))

		require.NoError(t, v.Validate(obj.Object()))
	})

	t.Run("tombstone content", func(t *testing.T) {
		obj := NewRaw()
		obj.SetType(object.TypeTombstone)

		require.Error(t, v.ValidateContent(obj.Object()))

		content := object.NewTombstone()
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
}
