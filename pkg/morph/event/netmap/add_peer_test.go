package netmap

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseAddPeer(t *testing.T) {
	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []smartcontract.Parameter{
			{},
			{},
		}

		_, err := ParseAddPeer(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(1, len(prms)).Error())
	})

	t.Run("wrong first parameter type", func(t *testing.T) {
		_, err := ParseAddPeer([]smartcontract.Parameter{
			{
				Type: smartcontract.ByteArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		info := []byte{1, 2, 3}

		ev, err := ParseAddPeer([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: info,
			},
		})

		require.NoError(t, err)
		require.Equal(t, AddPeer{
			node: info,
		}, ev)
	})
}
