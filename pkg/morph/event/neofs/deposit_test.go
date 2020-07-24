package neofs

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseDeposit(t *testing.T) {
	var (
		id   = []byte("Hello World")
		from = util.Uint160{0x1, 0x2, 0x3}
		to   = util.Uint160{0x3, 0x2, 0x1}

		amount int64 = 10
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []smartcontract.Parameter{
			{},
			{},
		}

		_, err := ParseDeposit(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(4, len(prms)).Error())
	})

	t.Run("wrong from parameter", func(t *testing.T) {
		_, err := ParseDeposit([]smartcontract.Parameter{
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("wrong amount parameter", func(t *testing.T) {
		_, err := ParseDeposit([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: from.BytesBE(),
			},
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("wrong to parameter", func(t *testing.T) {
		_, err := ParseDeposit([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: from.BytesBE(),
			},
			{
				Type:  smartcontract.IntegerType,
				Value: amount,
			},
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("wrong id parameter", func(t *testing.T) {
		_, err := ParseDeposit([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: from.BytesBE(),
			},
			{
				Type:  smartcontract.IntegerType,
				Value: amount,
			},
			{
				Type:  smartcontract.ByteArrayType,
				Value: to.BytesBE(),
			},
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParseDeposit([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: from.BytesBE(),
			},
			{
				Type:  smartcontract.IntegerType,
				Value: amount,
			},
			{
				Type:  smartcontract.ByteArrayType,
				Value: to.BytesBE(),
			},
			{
				Type:  smartcontract.ByteArrayType,
				Value: id,
			},
		})

		require.NoError(t, err)
		require.Equal(t, Deposit{
			id:     id,
			amount: amount,
			from:   from,
			to:     to,
		}, ev)
	})
}
