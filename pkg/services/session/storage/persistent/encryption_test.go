package persistent

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

	ts, err := NewTokenStore(filepath.Join(t.TempDir(), ".storage"), WithEncryptionKey(&pk.PrivateKey))
	require.NoError(t, err)

	data := []byte("nice encryption, awesome tests")

	encryptedData, err := ts.encrypt(data)
	require.NoError(t, err)
	require.False(t, bytes.Equal(data, encryptedData))

	decryptedData, err := ts.decrypt(encryptedData)
	require.NoError(t, err)

	require.Equal(t, data, decryptedData)
}
