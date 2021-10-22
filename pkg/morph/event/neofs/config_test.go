package neofs

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	var (
		id    = []byte("id")
		key   = []byte("key")
		value = []byte("value")
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
		}

		_, err := ParseConfig(createNotifyEventFromItems(prms))
		require.EqualError(t, err, event.WrongNumberOfParameters(3, len(prms)).Error())
	})

	t.Run("wrong first parameter", func(t *testing.T) {
		_, err := ParseConfig(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong second parameter", func(t *testing.T) {
		_, err := ParseConfig(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong third parameter", func(t *testing.T) {
		_, err := ParseConfig(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewByteArray(key),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("correct", func(t *testing.T) {
		ev, err := ParseConfig(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewByteArray(key),
			stackitem.NewByteArray(value),
		}))
		require.NoError(t, err)

		require.Equal(t, Config{
			id:    id,
			key:   key,
			value: value,
		}, ev)
	})
}
