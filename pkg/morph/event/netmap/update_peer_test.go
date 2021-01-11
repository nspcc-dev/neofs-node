package netmap

import (
	"crypto/elliptic"
	"math/big"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-crypto/test"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseUpdatePeer(t *testing.T) {
	var (
		publicKey = &test.DecodeKey(-1).PublicKey
		state     = netmap.NodeStateOffline
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
		}

		_, err := ParseUpdatePeer(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(2, len(prms)).Error())
	})

	t.Run("wrong first parameter type", func(t *testing.T) {
		_, err := ParseUpdatePeer([]stackitem.Item{
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong second parameter type", func(t *testing.T) {
		_, err := ParseUpdatePeer([]stackitem.Item{
			stackitem.NewByteArray(crypto.MarshalPublicKey(publicKey)),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParseUpdatePeer([]stackitem.Item{
			stackitem.NewBigInteger(new(big.Int).SetInt64(int64(state.ToV2()))),
			stackitem.NewByteArray(crypto.MarshalPublicKey(publicKey)),
		})
		require.NoError(t, err)

		expectedKey, err := keys.NewPublicKeyFromBytes(crypto.MarshalPublicKey(publicKey), elliptic.P256())
		require.NoError(t, err)

		require.Equal(t, UpdatePeer{
			publicKey: expectedKey,
			status:    state,
		}, ev)
	})
}
