package container

import (
	"context"
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/container"
	libcnr "github.com/nspcc-dev/neofs-node/lib/container"
	"github.com/nspcc-dev/neofs-node/lib/netmap"
	"github.com/nspcc-dev/neofs-node/lib/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Entity for mocking interfaces.
// Implementation of any interface intercepts arguments via f (if not nil).
// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
type testPutEntity struct {
	// Set of interfaces which entity must implement, but some methods from those does not call.
	libcnr.Storage

	// Argument interceptor. Used for ascertain of correct parameter passage between components.
	f func(...interface{})
	// Mocked result of any interface.
	res interface{}
	// Mocked error of any interface.
	err error
}

func (s testPutEntity) PutContainer(p libcnr.PutParams) (*libcnr.PutResult, error) {
	if s.f != nil {
		s.f(p)
	}

	if s.err != nil {
		return nil, s.err
	}

	return s.res.(*libcnr.PutResult), nil
}

func TestCnrService_Put(t *testing.T) {
	ctx := context.TODO()

	t.Run("unhealthy", func(t *testing.T) {
		s := cnrService{
			healthy: &testCommonEntity{
				err: errors.New("some error"),
			},
		}

		_, err := s.Put(ctx, new(container.PutRequest))
		require.Error(t, err)
	})

	t.Run("invalid request structure", func(t *testing.T) {
		s := cnrService{
			healthy: new(testCommonEntity),
		}

		// create unsigned request
		req := new(container.PutRequest)
		require.Error(t, requestVerifyFunc(req))

		_, err := s.Put(ctx, req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("container storage failure", func(t *testing.T) {
		req := new(container.PutRequest)
		req.SetCapacity(1)
		req.SetBasicACL(2)
		req.SetOwnerID(OwnerID{1, 2, 3})
		req.SetRules(netmap.PlacementRule{
			ReplFactor: 3,
		})

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		s := cnrService{
			healthy: new(testCommonEntity),
			cnrStore: &testPutEntity{
				f: func(items ...interface{}) {
					p := items[0].(libcnr.PutParams)
					require.Equal(t, ctx, p.Context())

					cnr := p.Container()
					require.Equal(t, req.GetCapacity(), cnr.GetCapacity())
					require.Equal(t, req.GetBasicACL(), cnr.GetBasicACL())
					require.Equal(t, req.GetRules(), cnr.GetRules())
					require.Equal(t, req.GetOwnerID(), cnr.OwnerID)
				},
				err: errors.New("storage error"),
			},
		}

		_, err := s.Put(ctx, req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Aborted, st.Code())
	})

	t.Run("correct result", func(t *testing.T) {
		req := new(container.PutRequest)

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		cid := CID{1, 2, 3}

		putRes := new(libcnr.PutResult)
		putRes.SetCID(cid)

		s := cnrService{
			healthy: new(testCommonEntity),
			cnrStore: &testPutEntity{
				res: putRes,
			},
		}

		res, err := s.Put(ctx, req)
		require.NoError(t, err)
		require.Equal(t, cid, res.CID)
	})
}
