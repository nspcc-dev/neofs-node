package netmap

import (
	"crypto/elliptic"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-crypto/test"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseUpdatePeer(t *testing.T) {
	var (
		publicKey       = &test.DecodeKey(-1).PublicKey
		state     int64 = 1
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []smartcontract.Parameter{
			{},
		}

		_, err := ParseUpdatePeer(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(2, len(prms)).Error())
	})

	t.Run("wrong first parameter type", func(t *testing.T) {
		_, err := ParseUpdatePeer([]smartcontract.Parameter{
			{
				Type: smartcontract.ByteArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("wrong second parameter type", func(t *testing.T) {
		_, err := ParseUpdatePeer([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: crypto.MarshalPublicKey(publicKey),
			},
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParseUpdatePeer([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: crypto.MarshalPublicKey(publicKey),
			},
			{
				Type:  smartcontract.IntegerType,
				Value: state,
			},
		})
		require.NoError(t, err)

		expectedKey, err := keys.NewPublicKeyFromBytes(crypto.MarshalPublicKey(publicKey), elliptic.P256())
		require.NoError(t, err)

		require.Equal(t, UpdatePeer{
			publicKey: expectedKey,
			status:    uint32(state),
		}, ev)
	})
}
