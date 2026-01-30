package util_test

import (
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestNewKeyStorage(t *testing.T) {
	nodeKey, err := keys.NewPrivateKey()
	require.NoError(t, err)

	tokenStor, err := state.NewPersistentStorage(path.Join(t.TempDir(), "storage"), true)
	require.NoError(t, err)
	stor := util.NewKeyStorage(&nodeKey.PrivateKey, tokenStor, mockedNetworkState{42})

	owner := usertest.ID()

	t.Run("node key", func(t *testing.T) {
		key, err := stor.GetKey(nil)
		require.NoError(t, err)
		require.Equal(t, nodeKey.PrivateKey, *key)
	})

	t.Run("unknown token", func(t *testing.T) {
		_, err = stor.GetKey(&util.SessionInfo{
			ID:    uuid.New(),
			Owner: usertest.ID(),
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

func createToken(t *testing.T, store *state.PersistentStorage, owner user.ID, exp uint64) session.Object {
	key := neofscryptotest.ECDSAPrivateKey()
	id := uuid.New()
	err := store.Store(key, owner, id[:], exp)
	require.NoError(t, err)

	var tok session.Object
	tok.SetAuthKey((*neofsecdsa.PublicKey)(&key.PublicKey))
	tok.SetID(id)

	return tok
}

type mockedNetworkState struct {
	value uint64
}

func (m mockedNetworkState) CurrentEpoch() uint64 {
	return m.value
}
