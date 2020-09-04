package container

import (
	"crypto/elliptic"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/stretchr/testify/require"
)

func TestParsePut(t *testing.T) {
	var (
		containerData = []byte("containerData")
		signature     = []byte("signature")
		publicKey     = &test.DecodeKey(-1).PublicKey
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []smartcontract.Parameter{
			{},
			{},
		}

		_, err := ParsePut(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(3, len(prms)).Error())
	})

	t.Run("wrong container parameter", func(t *testing.T) {
		_, err := ParsePut([]smartcontract.Parameter{
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("wrong signature parameter", func(t *testing.T) {
		_, err := ParsePut([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: containerData,
			},
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("wrong key parameter", func(t *testing.T) {
		_, err := ParsePut([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: containerData,
			},
			{
				Type:  smartcontract.ByteArrayType,
				Value: signature,
			},
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParsePut([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: containerData,
			},
			{
				Type:  smartcontract.ByteArrayType,
				Value: signature,
			},
			{
				Type:  smartcontract.ByteArrayType,
				Value: crypto.MarshalPublicKey(publicKey),
			},
		})
		require.NoError(t, err)

		expectedKey, err := keys.NewPublicKeyFromBytes(crypto.MarshalPublicKey(publicKey), elliptic.P256())
		require.NoError(t, err)

		require.Equal(t, Put{
			rawContainer: containerData,
			signature:    signature,
			publicKey:    expectedKey,
		}, ev)
	})
}
