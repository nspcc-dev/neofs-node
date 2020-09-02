package container

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

func TestParseDelete(t *testing.T) {
	var (
		containerID = []byte("containreID")
		signature   = []byte("signature")
	)

	t.Run("wrong number of parameters", func(t *testing.T) {
		prms := []smartcontract.Parameter{
			{},
		}

		_, err := ParseDelete(prms)
		require.EqualError(t, err, event.WrongNumberOfParameters(2, len(prms)).Error())
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
		_, err := ParseDelete([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: containerID,
			},
			{
				Type: smartcontract.ArrayType,
			},
		})

		require.Error(t, err)
	})

	t.Run("correct behavior", func(t *testing.T) {
		ev, err := ParseDelete([]smartcontract.Parameter{
			{
				Type:  smartcontract.ByteArrayType,
				Value: containerID,
			},
			{
				Type:  smartcontract.ByteArrayType,
				Value: signature,
			},
		})

		require.NoError(t, err)

		require.Equal(t, Delete{
			containerID: containerID,
			signature:   signature,
		}, ev)
	})
}
