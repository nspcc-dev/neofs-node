package reputation

import (
	"math/big"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/stretchr/testify/require"
)

func TestParsePut(t *testing.T) {
	var (
		epoch uint64 = 42

		peerID = reputation.PeerIDFromBytes([]byte("peerID"))
		value  = []byte("There should be marshalled structure")
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
		}

		_, err := ParsePut(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(3, len(prms)).Error())
	})

	t.Run("wrong epoch parameter", func(t *testing.T) {
		_, err := ParsePut([]stackitem.Item{
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong peerID parameter", func(t *testing.T) {
		_, err := ParsePut([]stackitem.Item{
			stackitem.NewBigInteger(new(big.Int).SetUint64(epoch)),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong value parameter", func(t *testing.T) {
		_, err := ParsePut([]stackitem.Item{
			stackitem.NewBigInteger(new(big.Int).SetUint64(epoch)),
			stackitem.NewByteArray(peerID.Bytes()),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParsePut([]stackitem.Item{
			stackitem.NewBigInteger(new(big.Int).SetUint64(epoch)),
			stackitem.NewByteArray(peerID.Bytes()),
			stackitem.NewByteArray(value),
		})
		require.NoError(t, err)

		require.Equal(t, Put{
			epoch:  epoch,
			peerID: peerID,
			value:  value,
		}, ev)
	})
}
