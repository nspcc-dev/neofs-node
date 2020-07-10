package netmap

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neofs-node/lib/blockchain/event"
	"github.com/stretchr/testify/require"
)

func TestParseNewEpoch(t *testing.T) {
	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []smartcontract.Parameter{
			{},
			{},
		}

		_, err := ParseNewEpoch(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(1, len(prms)).Error())
	})

	t.Run("wrong first parameter type", func(t *testing.T) {
		_, err := ParseNewEpoch([]smartcontract.Parameter{
			{
				Type: smartcontract.ByteArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		epochNum := uint64(100)

		ev, err := ParseNewEpoch([]smartcontract.Parameter{
			{
				Type:  smartcontract.IntegerType,
				Value: int64(epochNum),
			},
		})

		require.NoError(t, err)
		require.Equal(t, NewEpoch{
			num: epochNum,
		}, ev)
	})
}
