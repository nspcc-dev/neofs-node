package neofs

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func genKey(t *testing.T) *keys.PrivateKey {
	priv, err := keys.NewPrivateKey()
	require.NoError(t, err)
	return priv
}

func TestParseUpdateInnerRing(t *testing.T) {
	var (
		publicKeys = []*keys.PublicKey{
			genKey(t).PublicKey(),
			genKey(t).PublicKey(),
			genKey(t).PublicKey(),
		}
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []stackitem.Item{
			stackitem.NewMap(),
			stackitem.NewMap(),
		}

		_, err := ParseUpdateInnerRing(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(1, len(prms)).Error())
	})

	t.Run("wrong first parameter", func(t *testing.T) {
		_, err := ParseUpdateInnerRing([]stackitem.Item{
			stackitem.NewMap(),
		})

		require.Error(t, err)
	})

	t.Run("correct", func(t *testing.T) {
		ev, err := ParseUpdateInnerRing([]stackitem.Item{
			stackitem.NewArray([]stackitem.Item{
				stackitem.NewByteArray(publicKeys[0].Bytes()),
				stackitem.NewByteArray(publicKeys[1].Bytes()),
				stackitem.NewByteArray(publicKeys[2].Bytes()),
			}),
		})
		require.NoError(t, err)

		require.Equal(t, UpdateInnerRing{
			keys: publicKeys,
		}, ev)
	})
}
