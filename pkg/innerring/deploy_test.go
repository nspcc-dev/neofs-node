package innerring

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	"github.com/stretchr/testify/require"
)

func newTestPersistentStorage(tb testing.TB) *state.PersistentStorage {
	ps, err := state.NewPersistentStorage(filepath.Join(tb.TempDir(), "storage"))
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_ = ps.Close()
	})

	return ps
}

func TestSidechainKeyStorage_GetPersistedPrivateKey(t *testing.T) {
	testPersistedKey := func(tb testing.TB, ks *sidechainKeyStorage, persistedKey *keys.PrivateKey) {
		key, err := ks.GetPersistedPrivateKey()
		require.NoError(tb, err)
		require.Equal(tb, persistedKey, key)
	}

	t.Run("fresh", func(t *testing.T) {
		ks := newSidechainKeyStorage(newTestPersistentStorage(t))

		initKey, err := ks.GetPersistedPrivateKey()
		require.NoError(t, err)
		require.NotNil(t, initKey)

		testPersistedKey(t, ks, initKey)
	})

	t.Run("preset", func(t *testing.T) {
		ps := newTestPersistentStorage(t)

		acc, err := wallet.NewAccount()
		require.NoError(t, err)

		err = acc.Encrypt("", keys.NEP2ScryptParams())
		require.NoError(t, err)

		jWallet, err := json.Marshal(wallet.Wallet{
			Version:  "1.0",
			Accounts: []*wallet.Account{acc},
			Scrypt:   keys.ScryptParams{},
			Extra:    wallet.Extra{},
		})
		require.NoError(t, err)

		err = ps.SetBytes(committeeGroupKey, jWallet)
		require.NoError(t, err)

		ks := newSidechainKeyStorage(ps)

		testPersistedKey(t, ks, acc.PrivateKey())
	})
}
