package netmap

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result/subscriptions"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseAddPeer(t *testing.T) {
	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
		}

		_, err := ParseAddPeer(createNotifyEventFromItems(prms))
		require.EqualError(t, err, event.WrongNumberOfParameters(1, len(prms)).Error())
	})

	t.Run("wrong first parameter type", func(t *testing.T) {
		_, err := ParseAddPeer(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		info := []byte{1, 2, 3}

		ev, err := ParseAddPeer(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(info),
		}))

		require.NoError(t, err)
		require.Equal(t, AddPeer{
			node: info,
		}, ev)
	})
}

func createNotifyEventFromItems(items []stackitem.Item) *subscriptions.NotificationEvent {
	return &subscriptions.NotificationEvent{
		NotificationEvent: state.NotificationEvent{
			Item: stackitem.NewArray(items),
		},
	}
}
