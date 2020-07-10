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
type testGetEntity struct {
	// Set of interfaces which entity must implement, but some methods from those does not call.
	libcnr.Storage

	// Argument interceptor. Used for ascertain of correct parameter passage between components.
	f func(...interface{})
	// Mocked result of any interface.
	res interface{}
	// Mocked error of any interface.
	err error
}

func (s testGetEntity) GetContainer(p libcnr.GetParams) (*libcnr.GetResult, error) {
	if s.f != nil {
		s.f(p)
	}

	if s.err != nil {
		return nil, s.err
	}

	return s.res.(*libcnr.GetResult), nil
}

func TestCnrService_Get(t *testing.T) {
	ctx := context.TODO()

	t.Run("unhealthy", func(t *testing.T) {
		s := cnrService{
			healthy: &testCommonEntity{
				err: errors.New("some error"),
			},
		}

		_, err := s.Get(ctx, new(container.GetRequest))
		require.Error(t, err)
	})

	t.Run("invalid request structure", func(t *testing.T) {
		s := cnrService{
			healthy: new(testCommonEntity),
		}

		// create unsigned request
		req := new(container.GetRequest)
		require.Error(t, requestVerifyFunc(req))

		_, err := s.Get(ctx, req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("container storage failure", func(t *testing.T) {
		req := new(container.GetRequest)
		req.SetCID(CID{1, 2, 3})

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		s := cnrService{
			healthy: new(testCommonEntity),
			cnrStore: &testGetEntity{
				f: func(items ...interface{}) {
					p := items[0].(libcnr.GetParams)
					require.Equal(t, ctx, p.Context())
					require.Equal(t, req.GetCID(), p.CID())
				},
				err: errors.New("storage error"),
			},
		}

		_, err := s.Get(ctx, req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("correct result", func(t *testing.T) {
		req := new(container.GetRequest)

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		cnr := &Container{
			Capacity: 1,
		}

		getRes := new(libcnr.GetResult)
		getRes.SetContainer(cnr)

		s := cnrService{
			healthy: new(testCommonEntity),
			cnrStore: &testGetEntity{
				res: getRes,
			},
		}

		res, err := s.Get(ctx, req)
		require.NoError(t, err)
		require.Equal(t, cnr, res.GetContainer())
	})
}
