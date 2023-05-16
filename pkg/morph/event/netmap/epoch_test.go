package netmap

import (
	"math/big"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
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

		_, err := ParseNewEpoch(createNotifyEventFromItems(prms))
		require.EqualError(t, err, event.WrongNumberOfParameters(1, len(prms)).Error())
	})

	t.Run("wrong first parameter type", func(t *testing.T) {
		_, err := ParseNewEpoch(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		epochNum := uint64(100)

		ev, err := ParseNewEpoch(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewBigInteger(new(big.Int).SetUint64(epochNum)),
		}))

		require.NoError(t, err)
		require.Equal(t, NewEpoch{
			num: epochNum,
		}, ev)
	})
}

func createNotifyEventFromItems(items []stackitem.Item) *state.ContainedNotificationEvent {
	return &state.ContainedNotificationEvent{
		NotificationEvent: state.NotificationEvent{
			Item: stackitem.NewArray(items),
		},
	}
}
