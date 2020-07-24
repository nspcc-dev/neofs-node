package neofs

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseCheque(t *testing.T) {
	var (
		id   = []byte("Hello World")
		user = util.Uint160{0x1, 0x2, 0x3}
		lock = util.Uint160{0x3, 0x2, 0x1}

		amount int64 = 10
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []smartcontract.Parameter{
			{},
			{},
		}

		_, err := ParseCheque(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(4, len(prms)).Error())
	})

	t.Run("wrong id parameter", func(t *testing.T) {
		_, err := ParseCheque([]smartcontract.Parameter{
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("wrong user parameter", func(t *testing.T) {
		_, err := ParseCheque([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: id,
			},
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("wrong amount parameter", func(t *testing.T) {
		_, err := ParseCheque([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: id,
			},
			{
				Type:  smartcontract.ByteArrayType,
				Value: user.BytesBE(),
			},
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("wrong lock parameter", func(t *testing.T) {
		_, err := ParseCheque([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: id,
			},
			{
				Type:  smartcontract.ByteArrayType,
				Value: user.BytesBE(),
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

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParseCheque([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: id,
			},
			{
				Type:  smartcontract.ByteArrayType,
				Value: user.BytesBE(),
			},
			{
				Type:  smartcontract.IntegerType,
				Value: amount,
			},
			{
				Type:  smartcontract.ByteArrayType,
				Value: lock.BytesBE(),
			},
		})

		require.NoError(t, err)
		require.Equal(t, Cheque{
			id:     id,
			amount: amount,
			user:   user,
			lock:   lock,
		}, ev)
	})
}
