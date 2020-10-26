package netmap

import (
	"math/big"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseNewEpoch(t *testing.T) {
	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
		}

		_, err := ParseNewEpoch(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(1, len(prms)).Error())
	})

	t.Run("wrong first parameter type", func(t *testing.T) {
		_, err := ParseNewEpoch([]stackitem.Item{
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		epochNum := uint64(100)

		ev, err := ParseNewEpoch([]stackitem.Item{
			stackitem.NewBigInteger(new(big.Int).SetUint64(epochNum)),
		})

		require.NoError(t, err)
		require.Equal(t, NewEpoch{
			num: epochNum,
		}, ev)
	})
}
