package logicerr

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestError(t *testing.T) {
	t.Run("errors.Is", func(t *testing.T) {
		e1 := errors.New("some error")
		ee := Wrap(e1)
		require.ErrorIs(t, ee, e1)

		e2 := fmt.Errorf("wrap: %w", e1)
		ee = Wrap(e2)
		require.ErrorIs(t, ee, e1)
		require.ErrorIs(t, ee, e2)
	})

	t.Run("errors.As", func(t *testing.T) {
		e1 := testError{42}
		ee := Wrap(e1)

		{
			var actual testError
			require.ErrorAs(t, ee, &actual)
			require.Equal(t, e1.data, actual.data)
		}

		e2 := fmt.Errorf("wrap: %w", e1)
		ee = Wrap(e2)

		{
			var actual testError
			require.ErrorAs(t, ee, &actual)
			require.Equal(t, e1.data, actual.data)
		}
	})
}

type testError struct {
	data uint64
}

func (e testError) Error() string {
	return strconv.FormatUint(e.data, 10)
}
