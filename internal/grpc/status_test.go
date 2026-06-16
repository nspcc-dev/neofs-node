package grpc_test

import (
	"context"
	"errors"
	"testing"

	igrpc "github.com/nspcc-dev/neofs-node/internal/grpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
)

func TestConvertContextStatus(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		err := igrpc.ConvertContextStatus(nil)
		require.NoError(t, err)
	})

	t.Run("context", func(t *testing.T) {
		for _, err := range []error{
			context.Canceled,
			context.DeadlineExceeded,
		} {
			t.Run(err.Error(), func(t *testing.T) {
				st := status.FromContextError(err)
				got := igrpc.ConvertContextStatus(st.Err())
				require.ErrorIs(t, got, err)
			})
		}
	})

	t.Run("non-context", func(t *testing.T) {
		anyErr := errors.New("anyErr")

		t.Run("status", func(t *testing.T) {
			stErr := status.FromContextError(anyErr).Err()
			got := igrpc.ConvertContextStatus(stErr)
			require.Equal(t, got, stErr)
		})

		t.Run("non-status", func(t *testing.T) {
			got := igrpc.ConvertContextStatus(anyErr)
			require.Equal(t, got, anyErr)
		})
	})
}
