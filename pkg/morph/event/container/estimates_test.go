package container

import (
	"math/big"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestStartEstimation(t *testing.T) {
	var epochNum uint64 = 100
	epochItem := stackitem.NewBigInteger(new(big.Int).SetUint64(epochNum))

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
		}

		_, err := ParseStartEstimation(createNotifyEventFromItems(prms))
		require.EqualError(t, err, event.WrongNumberOfParameters(1, len(prms)).Error())
	})

	t.Run("wrong estimation parameter", func(t *testing.T) {
		_, err := ParseStartEstimation(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParseStartEstimation(createNotifyEventFromItems([]stackitem.Item{
			epochItem,
		}))

		require.NoError(t, err)

		require.Equal(t, StartEstimation{
			epochNum,
		}, ev)
	})
}

func TestStopEstimation(t *testing.T) {
	var epochNum uint64 = 100
	epochItem := stackitem.NewBigInteger(new(big.Int).SetUint64(epochNum))

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
		}

		_, err := ParseStopEstimation(createNotifyEventFromItems(prms))
		require.EqualError(t, err, event.WrongNumberOfParameters(1, len(prms)).Error())
	})

	t.Run("wrong estimation parameter", func(t *testing.T) {
		_, err := ParseStopEstimation(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParseStopEstimation(createNotifyEventFromItems([]stackitem.Item{
			epochItem,
		}))

		require.NoError(t, err)

		require.Equal(t, StopEstimation{
			epochNum,
		}, ev)
	})
}
