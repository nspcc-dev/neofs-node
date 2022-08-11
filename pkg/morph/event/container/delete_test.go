package container

import (
	"crypto/sha256"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestParseDelete(t *testing.T) {
	var (
		containerID = []byte("containreID")
		signature   = []byte("signature")
		token       = []byte("token")
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
		}

		_, err := ParseDelete(createNotifyEventFromItems(prms))
		require.EqualError(t, err, event.WrongNumberOfParameters(3, len(prms)).Error())
	})

	t.Run("wrong container parameter", func(t *testing.T) {
		_, err := ParseDelete(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong signature parameter", func(t *testing.T) {
		_, err := ParseDelete(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(containerID),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("wrong session token parameter", func(t *testing.T) {
		_, err := ParseDelete(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(containerID),
			stackitem.NewByteArray(signature),
			stackitem.NewMap(),
		}))

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParseDelete(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(containerID),
			stackitem.NewByteArray(signature),
			stackitem.NewByteArray(token),
		}))

		require.NoError(t, err)

		require.Equal(t, Delete{
			containerID: containerID,
			signature:   signature,
			token:       token,
		}, ev)
	})
}

func TestParseDeleteSuccess(t *testing.T) {
	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
		}

		_, err := ParseDeleteSuccess(createNotifyEventFromItems(prms))
		require.EqualError(t, err, event.WrongNumberOfParameters(1, len(prms)).Error())
	})

	t.Run("wrong container parameter", func(t *testing.T) {
		_, err := ParseDeleteSuccess(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewMap(),
		}))

		require.Error(t, err)

		_, err = ParseDeleteSuccess(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray([]byte{1, 2, 3}),
		}))

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		id := cidtest.ID()

		binID := make([]byte, sha256.Size)
		id.Encode(binID)

		ev, err := ParseDeleteSuccess(createNotifyEventFromItems([]stackitem.Item{
			stackitem.NewByteArray(binID),
		}))

		require.NoError(t, err)

		require.Equal(t, DeleteSuccess{
			ID: id,
		}, ev)
	})
}
