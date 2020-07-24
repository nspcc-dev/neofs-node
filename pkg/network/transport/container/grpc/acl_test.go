package container

import (
	"context"
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/acl"
	eacl "github.com/nspcc-dev/neofs-api-go/acl/extended"
	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
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

func (s *testEACLEntity) GetEACL(cid CID) (Table, error) {
	if s.f != nil {
		s.f(cid)
	}

	if s.err != nil {
		return nil, s.err
	}

	return s.res.(Table), nil
}

func (s *testEACLEntity) PutEACL(cid CID, table Table, sig []byte) error {
	if s.f != nil {
		s.f(cid, table, sig)
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

	t.Run("EACL storage failure", func(t *testing.T) {
		record := new(acl.EACLRecord)
		record.SetAction(acl.EACLRecord_Allow)

		table := eacl.WrapTable(nil)
		table.SetRecords([]eacl.Record{eacl.WrapRecord(record)})

		req := new(container.SetExtendedACLRequest)
		req.SetID(CID{1, 2, 3})
		req.SetEACL(eacl.MarshalTable(table))
		req.SetSignature([]byte{4, 5, 6})

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		s := cnrService{
			healthy: new(testCommonEntity),
			aclStore: &testEACLEntity{
				f: func(items ...interface{}) {
					require.Equal(t, req.GetID(), items[0])
					require.Equal(t, req.GetSignature(), items[2])
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

	t.Run("EACL storage failure", func(t *testing.T) {
		req := new(container.GetExtendedACLRequest)
		req.SetID(CID{1, 2, 3})

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		s := cnrService{
			healthy: new(testCommonEntity),
			aclStore: &testEACLEntity{
				f: func(items ...interface{}) {
					require.Equal(t, req.GetID(), items[0])
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
		req.SetID(CID{1, 2, 3})

		require.NoError(t, requestSignFunc(test.DecodeKey(0), req))

		table := eacl.WrapTable(nil)

		record := new(acl.EACLRecord)
		record.SetAction(acl.EACLRecord_Allow)

		table.SetRecords([]eacl.Record{eacl.WrapRecord(record)})

		s := cnrService{
			healthy: new(testCommonEntity),
			aclStore: &testEACLEntity{
				res: table,
			},
		}

		res, err := s.GetExtendedACL(ctx, req)
		require.NoError(t, err)
		require.Equal(t, eacl.MarshalTable(table), res.GetEACL())
		require.Empty(t, res.GetSignature())
	})
}
