package balance

import (
	"math/big"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseLock(t *testing.T) {
	var (
		id   = []byte("Hello World")
		user = util.Uint160{0x1, 0x2, 0x3}
		lock = util.Uint160{0x3, 0x2, 0x1}

		amount int64 = 10
		until  int64 = 20
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
		}

		_, err := ParseLock(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(5, len(prms)).Error())
	})

	t.Run("wrong id parameter", func(t *testing.T) {
		_, err := ParseLock([]stackitem.Item{
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong from parameter", func(t *testing.T) {
		_, err := ParseLock([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong lock parameter", func(t *testing.T) {
		_, err := ParseLock([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewByteArray(user.BytesBE()),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong amount parameter", func(t *testing.T) {
		_, err := ParseLock([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewByteArray(user.BytesBE()),
			stackitem.NewByteArray(lock.BytesBE()),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong until parameter", func(t *testing.T) {
		_, err := ParseLock([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewByteArray(user.BytesBE()),
			stackitem.NewByteArray(lock.BytesBE()),
			stackitem.NewBigInteger(new(big.Int).SetInt64(amount)),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParseLock([]stackitem.Item{
			stackitem.NewByteArray(id),
			stackitem.NewByteArray(user.BytesBE()),
			stackitem.NewByteArray(lock.BytesBE()),
			stackitem.NewBigInteger(new(big.Int).SetInt64(amount)),
			stackitem.NewBigInteger(new(big.Int).SetInt64(until)),
		})

		require.NoError(t, err)
		require.Equal(t, Lock{
			id:     id,
			user:   user,
			lock:   lock,
			amount: amount,
			until:  until,
		}, ev)
	})
}
