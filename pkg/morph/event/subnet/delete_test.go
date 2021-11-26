package subnetevents_test

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	subnetevents "github.com/nspcc-dev/neofs-node/pkg/morph/event/subnet"
	"github.com/stretchr/testify/require"
)

func TestParseDelete(t *testing.T) {
	id := []byte("id")

	t.Run("wrong number of items", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewByteArray(nil),
			stackitem.NewByteArray(nil),
		}

		_, err := subnetevents.ParseDelete(createNotifyEventFromItems(prms))
		require.Error(t, err)
	})

	t.Run("wrong id item", func(t *testing.T) {
		_, err := subnetevents.ParseDelete(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := subnetevents.ParseDelete(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(id),
		}))
		require.NoError(t, err)

		v := ev.(subnetevents.Delete)

		require.Equal(t, id, v.ID())
	})
}
