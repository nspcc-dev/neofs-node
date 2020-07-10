package container

import (
	"context"
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/lib/acl"
	"github.com/nspcc-dev/neofs-node/lib/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Entity for mocking interfaces.
// Implementation of any interface intercepts arguments via f (if not nil).
// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
type testEACLEntity struct {
	// Set of interfaces which entity must implement, but some methods from those does not call.

	// Argument interceptor. Used for ascertain of correct parameter passage between components.
	f func(...interface{})
	// Mocked result of any interface.
	res interface{}
	// Mocked error of any interface.
	err error
}

var requestSignFunc = service.SignRequestData

func (s testEACLEntity) GetBinaryEACL(_ context.Context, key acl.BinaryEACLKey) (acl.BinaryEACLValue, error) {
	if s.f != nil {
		s.f(key)
	}

	if s.err != nil {
		return acl.BinaryEACLValue{}, s.err
	}

	return s.res.(acl.BinaryEACLValue), nil
}

func (s testEACLEntity) PutBinaryEACL(_ context.Context, key acl.BinaryEACLKey, val acl.BinaryEACLValue) error {
	if s.f != nil {
		s.f(key, val)
	}

	return s.err
}

func TestCnrService_SetExtendedACL(t *testing.T) {
	ctx := context.TODO()

	t.Run("unhealthy", func(t *testing.T) {
		s := cnrService{
			healthy: &testCommonEntity{
				err: errors.New("some error"),
			},
		}

		_, err := s.SetExtendedACL(ctx, new(container.SetExtendedACLRequest))
		require.Error(t, err)
	})

	t.Run("invalid request structure", func(t *testing.T) {
		s := cnrService{
			healthy: new(testCommonEntity),
		}

		// create unsigned request
		req := new(container.SetExtendedACLRequest)
		require.Error(t, requestVerifyFunc(req))

		_, err := s.SetExtendedACL(ctx, req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("binary EACL storage failure", func(t *testing.T) {
		req := new(container.SetExtendedACLRequest)
		req.SetID(CID{1, 2, 3})
		req.SetEACL([]byte{4, 5, 6})
		req.SetSignature([]byte{7, 8, 9})

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		s := cnrService{
			healthy: new(testCommonEntity),
			aclStore: &testEACLEntity{
				f: func(items ...interface{}) {
					key := items[0].(acl.BinaryEACLKey)
					require.Equal(t, req.GetID(), key.CID())

					val := items[1].(acl.BinaryEACLValue)
					require.Equal(t, req.GetEACL(), val.EACL())
					require.Equal(t, req.GetSignature(), val.Signature())
				},
				err: errors.New("storage error"),
			},
		}

		_, err := s.SetExtendedACL(ctx, req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Aborted, st.Code())
	})

	t.Run("correct result", func(t *testing.T) {
		req := new(container.SetExtendedACLRequest)

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		s := cnrService{
			healthy:  new(testCommonEntity),
			aclStore: new(testEACLEntity),
		}

		res, err := s.SetExtendedACL(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res)
	})
}

func TestCnrService_GetExtendedACL(t *testing.T) {
	ctx := context.TODO()

	t.Run("unhealthy", func(t *testing.T) {
		s := cnrService{
			healthy: &testCommonEntity{
				err: errors.New("some error"),
			},
		}

		_, err := s.GetExtendedACL(ctx, new(container.GetExtendedACLRequest))
		require.Error(t, err)
	})

	t.Run("invalid request structure", func(t *testing.T) {
		s := cnrService{
			healthy: new(testCommonEntity),
		}

		// create unsigned request
		req := new(container.GetExtendedACLRequest)
		require.Error(t, requestVerifyFunc(req))

		_, err := s.GetExtendedACL(ctx, req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("binary EACL storage failure", func(t *testing.T) {
		req := new(container.GetExtendedACLRequest)
		req.SetID(CID{1, 2, 3})

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		s := cnrService{
			healthy: new(testCommonEntity),
			aclStore: &testEACLEntity{
				f: func(items ...interface{}) {
					key := items[0].(acl.BinaryEACLKey)
					require.Equal(t, req.GetID(), key.CID())
				},
				err: errors.New("storage error"),
			},
		}

		_, err := s.GetExtendedACL(ctx, req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("correct result", func(t *testing.T) {
		req := new(container.GetExtendedACLRequest)

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		eacl := []byte{1, 2, 3}
		sig := []byte{4, 5, 6}

		val := acl.BinaryEACLValue{}
		val.SetEACL(eacl)
		val.SetSignature(sig)

		s := cnrService{
			healthy: new(testCommonEntity),
			aclStore: &testEACLEntity{
				res: val,
			},
		}

		res, err := s.GetExtendedACL(ctx, req)
		require.NoError(t, err)
		require.Equal(t, eacl, res.GetEACL())
		require.Equal(t, sig, res.GetSignature())
	})
}
