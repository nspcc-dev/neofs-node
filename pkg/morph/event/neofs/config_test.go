package neofs

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	var (
		key   = []byte("key")
		value = []byte("value")
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
		}

		_, err := ParseConfig(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(2, len(prms)).Error())
	})

	t.Run("wrong first parameter", func(t *testing.T) {
		_, err := ParseConfig([]stackitem.Item{
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong second parameter", func(t *testing.T) {
		_, err := ParseConfig([]stackitem.Item{
			stackitem.NewByteArray(key),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("correct", func(t *testing.T) {
		ev, err := ParseConfig([]stackitem.Item{
			stackitem.NewByteArray(key),
			stackitem.NewByteArray(value),
		})
		require.NoError(t, err)

		require.Equal(t, Config{
			key:   key,
			value: value,
		}, ev)
	})
}
