package errors_test

import (
	"errors"
	"fmt"
	"testing"

	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

type testError string

func (x testError) Error() string {
	return string(x)
}

func TestErrObjectID(t *testing.T) {
	id := oidtest.ID()
	err := ierrors.ObjectID(id)

	t.Run("errors.As", func(t *testing.T) {
		check := func(t *testing.T, err error) {
			var e ierrors.ObjectID
			require.ErrorAs(t, err, &e)
			require.EqualValues(t, id, e)
		}

		check(t, err)
		check(t, fmt.Errorf("some context: %w, %w", errors.New("any"), err))
	})

	require.Implements(t, new(error), err)
	require.EqualError(t, err, id.String())
}

func TestNewParentObjectError(t *testing.T) {
	cause := testError("some cause")
	err := ierrors.NewParentObjectError(cause)

	t.Run("errors.As", func(t *testing.T) {
		var target testError
		require.ErrorAs(t, err, &target)
		require.Equal(t, cause, target)
	})
	t.Run("errors.Is", func(t *testing.T) {
		require.ErrorIs(t, err, cause)
		require.ErrorIs(t, err, ierrors.ErrParentObject)
	})

	require.Implements(t, new(error), err)
	require.EqualError(t, err, "some cause")
}
