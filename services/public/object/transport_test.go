package object

import (
	"context"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testTransportEntity struct {
		// Set of interfaces which entity must implement, but some methods from those does not call.
		object.ServiceClient
		object.Service_PutClient

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var (
	_ object.ServiceClient     = (*testTransportEntity)(nil)
	_ object.Service_PutClient = (*testTransportEntity)(nil)
)

func (s *testTransportEntity) Send(*object.PutRequest) error { return s.err }

func (s *testTransportEntity) CloseAndRecv() (*object.PutResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*object.PutResponse), nil
}

func (s *testTransportEntity) Put(ctx context.Context, opts ...grpc.CallOption) (object.Service_PutClient, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(object.Service_PutClient), nil
}

func Test_putHandler(t *testing.T) {
	ctx := context.TODO()

	t.Run("return type correctness", func(t *testing.T) {
		addr := new(Address)
		*addr = testObjectAddress(t)

		srvClient := &testTransportEntity{
			res: &testTransportEntity{
				res: &object.PutResponse{
					Address: *addr,
				},
			},
		}

		putC := &putCaller{}

		res, err := putC.call(ctx, &putRequestSequence{PutRequest: new(object.PutRequest)}, &clientInfo{
			sc: srvClient,
		})
		require.NoError(t, err)

		// ascertain that value returns as expected
		require.Equal(t, addr, res)
	})
}
