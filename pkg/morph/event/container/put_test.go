package container

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParsePut(t *testing.T) {
	var (
		containerData = []byte("containerData")
		signature     = []byte("signature")
		publicKey     = []byte("pubkey")
		token         = []byte("token")
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
		}

		_, err := ParsePut(createNotifyEventFromItems(prms))
		require.EqualError(t, err, event.WrongNumberOfParameters(expectedItemNumPut, len(prms)).Error())
	})

	t.Run("wrong container parameter", func(t *testing.T) {
		_, err := ParsePut(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong signature parameter", func(t *testing.T) {
		_, err := ParsePut(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(containerData),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong key parameter", func(t *testing.T) {
		_, err := ParsePut(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(containerData),
			stackitem.NewByteArray(signature),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong session token parameter", func(t *testing.T) {
		_, err := ParsePut(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(containerData),
			stackitem.NewByteArray(signature),
			stackitem.NewByteArray(publicKey),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParsePut(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(containerData),
			stackitem.NewByteArray(signature),
			stackitem.NewByteArray(publicKey),
			stackitem.NewByteArray(token),
		}))
		require.NoError(t, err)

		require.Equal(t, Put{
			rawContainer: containerData,
			signature:    signature,
			publicKey:    publicKey,
			token:        token,
		}, ev)
	})
}
