package deploy

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/stretchr/testify/require"
)

func TestKeySharing(t *testing.T) {
	coderKey, err := keys.NewPrivateKey()
	require.NoError(t, err)
	decoderKey, err := keys.NewPrivateKey()
	require.NoError(t, err)
	sharedKey, err := keys.NewPrivateKey()
	require.NoError(t, err)

	cipher, err := encryptSharedPrivateKey(sharedKey, coderKey, decoderKey.PublicKey())
	require.NoError(t, err)

	restoredKey, err := decryptSharedPrivateKey(cipher, coderKey.PublicKey(), decoderKey)
	require.NoError(t, err)

	require.Equal(t, sharedKey, restoredKey)
}
