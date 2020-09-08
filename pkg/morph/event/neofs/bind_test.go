package neofs

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/stretchr/testify/require"
)

func TestParseBind(t *testing.T) {
	var (
		user       = util.Uint160{0x1, 0x2, 0x3}
		publicKeys = []*ecdsa.PublicKey{
			&test.DecodeKey(1).PublicKey,
			&test.DecodeKey(2).PublicKey,
			&test.DecodeKey(3).PublicKey,
		}
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []smartcontract.Parameter{
			{},
		}

		_, err := ParseBind(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(2, len(prms)).Error())
	})

	t.Run("wrong first parameter", func(t *testing.T) {
		_, err := ParseBind([]smartcontract.Parameter{
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("wrong second parameter", func(t *testing.T) {
		_, err := ParseBind([]smartcontract.Parameter{
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

	t.Run("correct", func(t *testing.T) {
		ev, err := ParseBind([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: user.BytesBE(),
			},
			{
				Type: smartcontract.ArrayType,
				Value: []smartcontract.Parameter{
					{
						Type:  smartcontract.ByteArrayType,
						Value: crypto.MarshalPublicKey(publicKeys[0]),
					},
					{
						Type:  smartcontract.ByteArrayType,
						Value: crypto.MarshalPublicKey(publicKeys[1]),
					},
					{
						Type:  smartcontract.ByteArrayType,
						Value: crypto.MarshalPublicKey(publicKeys[2]),
					},
				},
			},
		})
		require.NoError(t, err)

		expKeys := make([]*keys.PublicKey, len(publicKeys))
		for i := range publicKeys {
			expKeys[i], err = keys.NewPublicKeyFromBytes(crypto.MarshalPublicKey(publicKeys[i]), elliptic.P256())
			require.NoError(t, err)
		}

		require.Equal(t, Bind{
			user: user,
			keys: expKeys,
		}, ev)
	})
}
