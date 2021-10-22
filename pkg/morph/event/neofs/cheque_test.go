package neofs

import (
	"math/big"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
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
		prms := []stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
		}

		_, err := ParseCheque(createNotifyEventFromItems(prms))
		require.EqualError(t, err, event.WrongNumberOfParameters(4, len(prms)).Error())
	})

	t.Run("wrong id parameter", func(t *testing.T) {
		_, err := ParseCheque(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong user parameter", func(t *testing.T) {
		_, err := ParseCheque(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong amount parameter", func(t *testing.T) {
		_, err := ParseCheque(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewByteArray(user.BytesBE()),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong lock parameter", func(t *testing.T) {
		_, err := ParseCheque(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewByteArray(user.BytesBE()),
			stackitem.NewBigInteger(new(big.Int).SetInt64(amount)),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParseCheque(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewByteArray(user.BytesBE()),
			stackitem.NewBigInteger(new(big.Int).SetInt64(amount)),
			stackitem.NewByteArray(lock.BytesBE()),
		}))

		require.NoError(t, err)
		require.Equal(t, Cheque{
			id:     id,
			amount: amount,
			user:   user,
			lock:   lock,
		}, ev)
	})
}
