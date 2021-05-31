package neofs

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseBind(t *testing.T) {
	var (
		user       = []byte{0x1, 0x2, 0x3}
		publicKeys = [][]byte{
			[]byte("key1"),
			[]byte("key2"),
			[]byte("key3"),
		}
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
		}

		_, err := ParseBind(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(2, len(prms)).Error())
	})

	t.Run("wrong first parameter", func(t *testing.T) {
		_, err := ParseBind([]stackitem.Item{
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong second parameter", func(t *testing.T) {
		_, err := ParseBind([]stackitem.Item{
			stackitem.NewByteArray(user),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("correct", func(t *testing.T) {
		ev, err := ParseBind([]stackitem.Item{
			stackitem.NewByteArray(user),
			stackitem.NewArray([]stackitem.Item{
				stackitem.NewByteArray(publicKeys[0]),
				stackitem.NewByteArray(publicKeys[1]),
				stackitem.NewByteArray(publicKeys[2]),
			}),
		})
		require.NoError(t, err)

		e := ev.(Bind)

		require.Equal(t, user, e.User())
		require.Equal(t, publicKeys, e.Keys())
	})
}
