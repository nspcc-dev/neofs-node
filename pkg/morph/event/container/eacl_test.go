package container_test

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/stretchr/testify/require"
)

func TestParseEACL(t *testing.T) {
	var (
		binaryTable = []byte("table")
		signature   = []byte("signature")
		publicKey   = []byte("pubkey")
		token       = []byte("token")
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		items := []stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
		}

		_, err := container.ParseSetEACL(items)
		require.EqualError(t, err, event.WrongNumberOfParameters(4, len(items)).Error())
	})

	t.Run("wrong container parameter", func(t *testing.T) {
		_, err := container.ParseSetEACL([]stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong signature parameter", func(t *testing.T) {
		_, err := container.ParseSetEACL([]stackitem.Item{
			stackitem.NewByteArray(binaryTable),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong key parameter", func(t *testing.T) {
		_, err := container.ParseSetEACL([]stackitem.Item{
			stackitem.NewByteArray(binaryTable),
			stackitem.NewByteArray(signature),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong session token parameter", func(t *testing.T) {
		_, err := container.ParseSetEACL([]stackitem.Item{
			stackitem.NewByteArray(binaryTable),
			stackitem.NewByteArray(signature),
			stackitem.NewByteArray(publicKey),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := container.ParseSetEACL([]stackitem.Item{
			stackitem.NewByteArray(binaryTable),
			stackitem.NewByteArray(signature),
			stackitem.NewByteArray(publicKey),
			stackitem.NewByteArray(token),
		})
		require.NoError(t, err)

		e := ev.(container.SetEACL)

		require.Equal(t, binaryTable, e.Table())
		require.Equal(t, signature, e.Signature())
		require.Equal(t, publicKey, e.PublicKey())
		require.Equal(t, token, e.SessionToken())
	})
}
