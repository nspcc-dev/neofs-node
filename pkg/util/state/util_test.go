package state

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/stretchr/testify/require"
)

func TestPack(t *testing.T) {
	key, err := keys.NewPrivateKey()
	require.NoError(t, err)

	ts := new(PersistentStorage)

	const exp = 12345

	raw, err := ts.packToken(exp, &key.PrivateKey)
	require.NoError(t, err)

	require.Equal(t, uint64(exp), epochFromToken(raw))

	unpacked, err := ts.unpackToken(raw)
	require.NoError(t, err)

	require.Equal(t, uint64(exp), unpacked.ExpiredAt())
	require.Equal(t, true, key.Equal(unpacked.SessionKey()))
}
