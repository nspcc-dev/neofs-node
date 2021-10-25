package util_test

import (
	"context"
	"crypto/elliptic"
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/pkg/session"
	sessionV2 "github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	tokenStorage "github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/stretchr/testify/require"
)

func TestNewKeyStorage(t *testing.T) {
	nodeKey, err := keys.NewPrivateKey()
	require.NoError(t, err)

	tokenStor := tokenStorage.New()
	stor := util.NewKeyStorage(&nodeKey.PrivateKey, tokenStor, mockedNetworkState{42})

	t.Run("node key", func(t *testing.T) {
		key, err := stor.GetKey(nil)
		require.NoError(t, err)
		require.Equal(t, nodeKey.PrivateKey, *key)
	})

	t.Run("unknown token", func(t *testing.T) {
		tok := generateToken(t)
		_, err = stor.GetKey(tok)
		require.Error(t, err)
	})

	t.Run("known token", func(t *testing.T) {
		tok := createToken(t, tokenStor, 100)
		pubKey, err := keys.NewPublicKeyFromBytes(tok.SessionKey(), elliptic.P256())
		require.NoError(t, err)

		key, err := stor.GetKey(tok)
		require.NoError(t, err)
		require.Equal(t, pubKey.X, key.PublicKey.X)
		require.Equal(t, pubKey.Y, key.PublicKey.Y)
	})

	t.Run("expired token", func(t *testing.T) {
		tok := createToken(t, tokenStor, 30)
		_, err := stor.GetKey(tok)
		require.Error(t, err)
	})
}

func generateToken(t *testing.T) *session.Token {
	key, err := keys.NewPrivateKey()
	require.NoError(t, err)

	pubKey := key.PublicKey().Bytes()
	id, err := uuid.New().MarshalBinary()
	require.NoError(t, err)

	tok := session.NewToken()
	tok.SetSessionKey(pubKey)
	tok.SetID(id)

	return tok
}

func createToken(t *testing.T, store *tokenStorage.TokenStore, exp uint64) *session.Token {
	req := new(sessionV2.CreateRequestBody)
	req.SetOwnerID(nil)
	req.SetExpiration(exp)

	resp, err := store.Create(context.Background(), req)
	require.NoError(t, err)

	tok := session.NewToken()
	tok.SetSessionKey(resp.GetSessionKey())
	tok.SetID(resp.GetID())

	return tok
}

type mockedNetworkState struct {
	value uint64
}

func (m mockedNetworkState) CurrentEpoch() uint64 {
	return m.value
}
