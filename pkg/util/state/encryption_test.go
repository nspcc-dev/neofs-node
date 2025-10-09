package state

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/stretchr/testify/require"
)

func TestTokenStore_Encryption(t *testing.T) {
	pk, err := keys.NewPrivateKey()
	require.NoError(t, err)

	ts := newStorageWithSession(t, filepath.Join(t.TempDir(), ".storage"), WithEncryptionKey(&pk.PrivateKey))

	data := []byte("nice encryption, awesome tests")

	encryptedData, err := ts.encrypt(data)
	require.NoError(t, err)
	require.False(t, bytes.Equal(data, encryptedData))

	decryptedData, err := ts.decrypt(encryptedData)
	require.NoError(t, err)

	require.Equal(t, data, decryptedData)
}

func newStorageWithSession(tb testing.TB, path string, opts ...Option) *PersistentStorage {
	storage, err := NewPersistentStorage(path, true, opts...)
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_ = storage.Close()
	})

	return storage
}
