package errors_test

import (
	"fmt"
	"testing"

	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	"github.com/stretchr/testify/require"
)

type testError int

func (x testError) Error() string {
	return fmt.Sprintf("error code #%d", x)
}

func TestTemporary(t *testing.T) {
	cause := testError(404)
	tmp := ierrors.Temporary{Cause: cause}
	t.Run("text", func(t *testing.T) {
		require.Equal(t, cause.Error(), tmp.Error())
	})
	t.Run("is", func(t *testing.T) {
		require.ErrorIs(t, tmp, cause)
	})
	t.Run("as", func(t *testing.T) {
		tmp := ierrors.Temporary{Cause: fmt.Errorf("any context: %w", cause)}
		var tgt testError
		require.ErrorAs(t, tmp, &tgt)
		require.Equal(t, cause, tgt)
	})
}
