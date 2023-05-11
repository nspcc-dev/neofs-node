package container

import (
	"crypto/sha256"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestParsePutSuccess(t *testing.T) {
	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
		}

		_, err := ParsePutSuccess(createNotifyEventFromItems(prms))
		require.EqualError(t, err, event.WrongNumberOfParameters(2, len(prms)).Error())
	})

	t.Run("wrong container ID parameter", func(t *testing.T) {
		_, err := ParsePutSuccess(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	id := cidtest.ID()

	binID := make([]byte, sha256.Size)
	id.Encode(binID)

	t.Run("wrong public key parameter", func(t *testing.T) {
		_, err := ParsePutSuccess(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(binID),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParsePutSuccess(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(binID),
			stackitem.NewByteArray([]byte("key")),
		}))

		require.NoError(t, err)

		require.Equal(t, PutSuccess{
			ID: id,
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
