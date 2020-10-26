package neofs

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-crypto/test"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseUpdateInnerRing(t *testing.T) {
	var (
		publicKeys = []*ecdsa.PublicKey{
			&test.DecodeKey(1).PublicKey,
			&test.DecodeKey(2).PublicKey,
			&test.DecodeKey(3).PublicKey,
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
				stackitem.NewByteArray(crypto.MarshalPublicKey(publicKeys[0])),
				stackitem.NewByteArray(crypto.MarshalPublicKey(publicKeys[1])),
				stackitem.NewByteArray(crypto.MarshalPublicKey(publicKeys[2])),
			}),
		})
		require.NoError(t, err)

		expKeys := make([]*keys.PublicKey, len(publicKeys))
		for i := range publicKeys {
			expKeys[i], err = keys.NewPublicKeyFromBytes(
				crypto.MarshalPublicKey(publicKeys[i]), elliptic.P256())
			require.NoError(t, err)
		}

		require.Equal(t, UpdateInnerRing{
			keys: expKeys,
		}, ev)
	})
}
