package container

import (
	"context"
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/container"
	libcnr "github.com/nspcc-dev/neofs-node/lib/container"
	"github.com/nspcc-dev/neofs-node/lib/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Entity for mocking interfaces.
// Implementation of any interface intercepts arguments via f (if not nil).
// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
type testDeleteEntity struct {
	// Set of interfaces which entity must implement, but some methods from those does not call.
	libcnr.Storage

	// Argument interceptor. Used for ascertain of correct parameter passage between components.
	f func(...interface{})
	// Mocked result of any interface.
	res interface{}
	// Mocked error of any interface.
	err error
}

func (s testDeleteEntity) DeleteContainer(p libcnr.DeleteParams) (*libcnr.DeleteResult, error) {
	if s.f != nil {
		s.f(p)
	}

	if s.err != nil {
		return nil, s.err
	}

	return s.res.(*libcnr.DeleteResult), nil
}

func TestCnrService_Delete(t *testing.T) {
	ctx := context.TODO()

	t.Run("unhealthy", func(t *testing.T) {
		s := cnrService{
			healthy: &testCommonEntity{
				err: errors.New("some error"),
			},
		}

		_, err := s.Delete(ctx, new(container.DeleteRequest))
		require.Error(t, err)
	})

	t.Run("invalid request structure", func(t *testing.T) {
		s := cnrService{
			healthy: new(testCommonEntity),
		}

		// create unsigned request
		req := new(container.DeleteRequest)
		require.Error(t, requestVerifyFunc(req))

		_, err := s.Delete(ctx, req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("container storage failure", func(t *testing.T) {
		req := new(container.DeleteRequest)
		req.SetCID(CID{1, 2, 3})

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		s := cnrService{
			healthy: new(testCommonEntity),
			cnrStore: &testDeleteEntity{
				f: func(items ...interface{}) {
					p := items[0].(libcnr.DeleteParams)
					require.Equal(t, ctx, p.Context())
					require.Equal(t, req.GetCID(), p.CID())
				},
				err: errors.New("storage error"),
			},
		}

		_, err := s.Delete(ctx, req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Aborted, st.Code())
	})

	t.Run("correct result", func(t *testing.T) {
		req := new(container.DeleteRequest)

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		delRes := new(libcnr.DeleteResult)

		s := cnrService{
			healthy: new(testCommonEntity),
			cnrStore: &testDeleteEntity{
				res: delRes,
			},
		}

		res, err := s.Delete(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res)
	})
}
