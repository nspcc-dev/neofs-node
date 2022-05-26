package util_test

import (
	"context"
	"crypto/elliptic"
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	sessionV2 "github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	tokenStorage "github.com/nspcc-dev/neofs-node/pkg/services/session/storage/temporary"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestNewKeyStorage(t *testing.T) {
	nodeKey, err := keys.NewPrivateKey()
	require.NoError(t, err)

	tokenStor := tokenStorage.NewTokenStore()
	stor := util.NewKeyStorage(&nodeKey.PrivateKey, tokenStor, mockedNetworkState{42})

	owner := *usertest.ID()

	t.Run("node key", func(t *testing.T) {
		key, err := stor.GetKey(nil)
		require.NoError(t, err)
		require.Equal(t, nodeKey.PrivateKey, *key)
	})

	t.Run("unknown token", func(t *testing.T) {
		_, err = stor.GetKey(&util.SessionInfo{
			ID:    uuid.New(),
			Owner: *usertest.ID(),
		})
		require.Error(t, err)
	})

	t.Run("known token", func(t *testing.T) {
		tok := createToken(t, tokenStor, owner, 100)

		key, err := stor.GetKey(&util.SessionInfo{
			ID:    tok.ID(),
			Owner: owner,
		})
		require.NoError(t, err)
		require.True(t, tok.AssertAuthKey((*neofsecdsa.PublicKey)(&key.PublicKey)))
	})

	t.Run("expired token", func(t *testing.T) {
		tok := createToken(t, tokenStor, owner, 30)
		_, err := stor.GetKey(&util.SessionInfo{
			ID:    tok.ID(),
			Owner: owner,
		})
		require.Error(t, err)
	})
}

func createToken(t *testing.T, store *tokenStorage.TokenStore, owner user.ID, exp uint64) session.Object {
	var ownerV2 refs.OwnerID
	owner.WriteToV2(&ownerV2)

	req := new(sessionV2.CreateRequestBody)
	req.SetOwnerID(&ownerV2)
	req.SetExpiration(exp)

	resp, err := store.Create(context.Background(), req)
	require.NoError(t, err)

	pub, err := keys.NewPublicKeyFromBytes(resp.GetSessionKey(), elliptic.P256())
	require.NoError(t, err)

	var id uuid.UUID
	require.NoError(t, id.UnmarshalBinary(resp.GetID()))

	var tok session.Object
	tok.SetAuthKey((*neofsecdsa.PublicKey)(pub))
	tok.SetID(id)

	return tok
}

type mockedNetworkState struct {
	value uint64
}

func (m mockedNetworkState) CurrentEpoch() uint64 {
	return m.value
}
