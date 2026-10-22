package state

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestTokenStore_Encryption(t *testing.T) {
	pk, err := keys.NewPrivateKey()
	require.NoError(t, err)

	ts := newStorageWithSession(t, filepath.Join(t.TempDir(), ".storage"), WithEncryptionKey(&pk.PrivateKey))

	data := []byte("nice encryption, awesome tests")

	encryptedData, err := ts.encrypt(data)
	require.NoError(t, err)

	t.Run("round trip", func(t *testing.T) {
		require.NotEqual(t, data, encryptedData)
		decrypted, err := ts.decrypt(encryptedData)
		require.NoError(t, err)
		require.Equal(t, data, decrypted)
	})

	t.Run("randomized encryption", func(t *testing.T) {
		encrypted, err := ts.encrypt(data)
		require.NoError(t, err)
		require.NotEqual(t, encryptedData, encrypted)
	})

	t.Run("wrong key", func(t *testing.T) {
		key, err := keys.NewPrivateKey()
		require.NoError(t, err)
		other := newStorageWithSession(t, filepath.Join(t.TempDir(), ".storage"), WithEncryptionKey(&key.PrivateKey))
		_, err = other.decrypt(encryptedData)
		require.Error(t, err)
	})

	t.Run("truncated ciphertext", func(t *testing.T) {
		for n := range encryptedData {
			_, err := ts.decrypt(encryptedData[:n])
			require.Error(t, err, "truncated to %d bytes", n)
		}
	})

	t.Run("corrupted ciphertext", func(t *testing.T) {
		for i := range encryptedData {
			corrupted := bytes.Clone(encryptedData)
			corrupted[i] ^= 1
			_, err := ts.decrypt(corrupted)
			require.Error(t, err, "corrupted byte %d", i)
		}
	})

	t.Run("persistence", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), ".storage")
		ts := newStorageWithSession(t, path, WithEncryptionKey(&pk.PrivateKey))
		// An empty encrypted store must also survive reopening.
		require.NoError(t, ts.Close())
		ts = newStorageWithSession(t, path, WithEncryptionKey(&pk.PrivateKey))

		account := usertest.User()
		require.NoError(t, ts.Store(account.ECDSAPrivateKey, 12345))
		before := storedSessions(t, ts)
		require.NoError(t, ts.Close())

		wrongKey, err := keys.NewPrivateKey()
		require.NoError(t, err)
		wrongStore := newStorageWithSession(t, path, WithEncryptionKey(&wrongKey.PrivateKey))
		require.Nil(t, wrongStore.GetToken(account.ID))
		require.Equal(t, before, storedSessions(t, wrongStore))
		require.NoError(t, wrongStore.Close())

		ts = newStorageWithSession(t, path, WithEncryptionKey(&pk.PrivateKey))
		require.Equal(t, before, storedSessions(t, ts))
		token := ts.GetToken(account.ID)
		require.NotNil(t, token)
		require.Equal(t, account.ECDSAPrivateKey, *token.SessionKey())
		require.EqualValues(t, 12345, token.ExpiredAt())

		subject := sessionv2.NewTargetUser(account.ID)
		require.Equal(t, token, ts.FindTokenBySubjects([]sessionv2.Target{subject}))
		ts.RemoveOldTokens(12344)
		require.NotNil(t, ts.GetToken(account.ID))
		ts.RemoveOldTokens(12345)
		require.Nil(t, ts.GetToken(account.ID))
	})
}

func storedSessions(t *testing.T, p *PersistentStorage) map[string][]byte {
	records := make(map[string][]byte)
	require.NoError(t, p.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(sessionsBucket).ForEach(func(k, v []byte) error {
			records[string(k)] = bytes.Clone(v)
			return nil
		})
	}))
	return records
}

func newStorageWithSession(tb testing.TB, path string, opts ...Option) *PersistentStorage {
	storage, err := NewPersistentStorage(path, true, opts...)
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_ = storage.Close()
	})

	return storage
}
