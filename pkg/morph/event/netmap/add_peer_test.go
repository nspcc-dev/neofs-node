package netmap

import (
	"testing"

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

		_, err := ParseAddPeer(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(1, len(prms)).Error())
	})

	t.Run("wrong first parameter type", func(t *testing.T) {
		_, err := ParseAddPeer([]stackitem.Item{
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		info := []byte{1, 2, 3}

		ev, err := ParseAddPeer([]stackitem.Item{
			stackitem.NewByteArray(info),
		})

		require.NoError(t, err)
		require.Equal(t, AddPeer{
			node: info,
		}, ev)
	})
}
