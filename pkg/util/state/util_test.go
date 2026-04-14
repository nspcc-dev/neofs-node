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

	gotEpoch, err := epochFromToken(raw)
	require.NoError(t, err)
	require.Equal(t, uint64(exp), gotEpoch)

	unpacked, err := ts.unpackToken(raw)
	require.NoError(t, err)

	require.Equal(t, uint64(exp), unpacked.ExpiredAt())
	require.Equal(t, true, key.Equal(unpacked.SessionKey()))

	zeroEpoch, err := epochFromToken(nil)
	require.ErrorIs(t, err, errInvalidPackedToken)
	require.Equal(t, uint64(0), zeroEpoch)

	nilTok, err := ts.unpackToken(nil)
	require.Nil(t, nilTok)
	require.ErrorIs(t, err, errInvalidPackedToken)
}
