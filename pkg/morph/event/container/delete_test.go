package container

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
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

		_, err := ParseDelete(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(3, len(prms)).Error())
	})

	t.Run("wrong container parameter", func(t *testing.T) {
		_, err := ParseDelete([]stackitem.Item{
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong signature parameter", func(t *testing.T) {
		_, err := ParseDelete([]stackitem.Item{
			stackitem.NewByteArray(containerID),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("wrong session token parameter", func(t *testing.T) {
		_, err := ParseDelete([]stackitem.Item{
			stackitem.NewByteArray(containerID),
			stackitem.NewByteArray(signature),
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParseDelete([]stackitem.Item{
			stackitem.NewByteArray(containerID),
			stackitem.NewByteArray(signature),
			stackitem.NewByteArray(token),
		})

		require.NoError(t, err)

		require.Equal(t, Delete{
			containerID: containerID,
			signature:   signature,
			token:       token,
		}, ev)
	})
}
