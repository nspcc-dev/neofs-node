package subnetevents_test

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	subnetevents "github.com/nspcc-dev/neofs-node/pkg/morph/event/subnet"
	"github.com/stretchr/testify/require"
)

func TestParsePut(t *testing.T) {
	var (
		id    = []byte("id")
		owner = []byte("owner")
		info  = []byte("info")
	)

	t.Run("wrong number of items", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewByteArray(nil),
			stackitem.NewByteArray(nil),
		}

		_, err := subnetevents.ParsePut(createNotifyEventFromItems(prms))
		require.Error(t, err)
	})

	t.Run("wrong id item", func(t *testing.T) {
		_, err := subnetevents.ParsePut(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong owner item", func(t *testing.T) {
		_, err := subnetevents.ParsePut(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong info item", func(t *testing.T) {
		_, err := subnetevents.ParsePut(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewByteArray(owner),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := subnetevents.ParsePut(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewByteArray(owner),
			stackitem.NewByteArray(info),
		}))
		require.NoError(t, err)

		v := ev.(subnetevents.Put)

		require.Equal(t, id, v.ID())
		require.Equal(t, owner, v.Owner())
		require.Equal(t, info, v.Info())
	})
}
