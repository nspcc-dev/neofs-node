package neofs

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	var (
		key   = []byte("key")
		value = []byte("value")
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []smartcontract.Parameter{
			{},
		}

		_, err := ParseConfig(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(2, len(prms)).Error())
	})

	t.Run("wrong first parameter", func(t *testing.T) {
		_, err := ParseConfig([]smartcontract.Parameter{
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("wrong second parameter", func(t *testing.T) {
		_, err := ParseConfig([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: key,
			},
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("correct", func(t *testing.T) {
		ev, err := ParseConfig([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: key,
			},
			{
				Type:  smartcontract.ByteArrayType,
				Value: value,
			},
		})
		require.NoError(t, err)

		require.Equal(t, Config{
			key:   key,
			value: value,
		}, ev)
	})
}
