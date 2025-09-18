package ec_test

import (
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestErrParts(t *testing.T) {
	ids := oidtest.IDs(10)
	err := iec.ErrParts(ids)

	t.Run("errors.As", func(t *testing.T) {
		var target iec.ErrParts
		require.ErrorAs(t, err, &target)
		require.EqualValues(t, ids, target)
	})

	require.Implements(t, new(error), err)
	require.EqualError(t, err, "10 EC parts")
}
