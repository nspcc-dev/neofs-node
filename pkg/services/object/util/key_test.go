package util_test

import (
	"path"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestNewKeyStorage(t *testing.T) {
	nodeKey, err := keys.NewPrivateKey()
	require.NoError(t, err)

	tokenStor, err := state.NewPersistentStorage(path.Join(t.TempDir(), "storage"), true)
	require.NoError(t, err)
	stor := util.NewKeyStorage(&nodeKey.PrivateKey, tokenStor, mockedNetworkState{42})

	t.Run("node key", func(t *testing.T) {
		key, err := stor.GetKey(nil)
		require.NoError(t, err)
		require.Equal(t, nodeKey.PrivateKey, *key)
	})

	t.Run("unknown token", func(t *testing.T) {
		unknownUser := usertest.ID()
		_, err = stor.GetKey(&unknownUser)
		require.Error(t, err)
	})

	t.Run("known token", func(t *testing.T) {
		tok := createToken(t, tokenStor, 100)

		authUser, err := tok.AuthUser()
		require.NoError(t, err)
		key, err := stor.GetKey(&authUser)
		require.NoError(t, err)
		require.True(t, tok.AssertAuthKey((*neofsecdsa.PublicKey)(&key.PublicKey)))
	})

	t.Run("expired token", func(t *testing.T) {
		tok := createToken(t, tokenStor, 30)
		authUser, err := tok.AuthUser()
		require.NoError(t, err)
		_, err = stor.GetKey(&authUser)
		require.Error(t, err)
	})
}

func createToken(t *testing.T, store *state.PersistentStorage, exp uint64) session.Object {
	key := neofscryptotest.ECDSAPrivateKey()
	err := store.Store(key, exp)
	require.NoError(t, err)

	var tok session.Object
	tok.SetAuthKey((*neofsecdsa.PublicKey)(&key.PublicKey))

	return tok
}

type mockedNetworkState struct {
	value uint64
}

func (m mockedNetworkState) CurrentEpoch() uint64 {
	return m.value
}
