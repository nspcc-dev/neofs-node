package object

import (
	"testing"

	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"github.com/stretchr/testify/require"
)

func TestGetRequestVerificationSignaturesCount(t *testing.T) {
	assert := func(t *testing.T, exp int, vers []*protorefs.Version) {
		for _, v := range vers {
			require.EqualValues(t, exp, getRequestVerificationSignaturesCount(v), v)
		}
	}

	t.Run("all", func(t *testing.T) {
		assert(t, 3, []*protorefs.Version{
			nil,
			new(protorefs.Version),
			{Major: 1, Minor: 26},
			{Major: 2, Minor: 24},
		})
	})

	t.Run("no origin", func(t *testing.T) {
		assert(t, 2, []*protorefs.Version{
			{Major: 2, Minor: 25},
		})
	})

	assert(t, 1, []*protorefs.Version{
		{Major: 2, Minor: 26},
		{Major: 3, Minor: 0},
	})
}
