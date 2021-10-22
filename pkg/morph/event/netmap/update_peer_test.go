package netmap

import (
	"math/big"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

func TestParseUpdatePeer(t *testing.T) {
	priv, err := keys.NewPrivateKey()
	require.NoError(t, err)

	var (
		publicKey = priv.PublicKey()
		state     = netmap.NodeStateOffline
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
		}

		_, err := ParseUpdatePeer(createNotifyEventFromItems(prms))
		require.EqualError(t, err, event.WrongNumberOfParameters(2, len(prms)).Error())
	})

	t.Run("wrong first parameter type", func(t *testing.T) {
		_, err := ParseUpdatePeer(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong second parameter type", func(t *testing.T) {
		_, err := ParseUpdatePeer(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(publicKey.Bytes()),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParseUpdatePeer(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewBigInteger(new(big.Int).SetInt64(int64(state.ToV2()))),
			stackitem.NewByteArray(publicKey.Bytes()),
		}))
		require.NoError(t, err)

		require.Equal(t, UpdatePeer{
			publicKey: publicKey,
			status:    state,
		}, ev)
	})
}
