package object

import (
	"context"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/object"
	v1 "github.com/nspcc-dev/neofs-api-go/query"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"github.com/stretchr/testify/require"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testSearchEntity struct {
		// Set of interfaces which entity must implement, but some methods from those does not call.
		object.Service_SearchServer

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var (
	_ requestHandler    = (*testSearchEntity)(nil)
	_ operationExecutor = (*testSearchEntity)(nil)
	_ responsePreparer  = (*testSearchEntity)(nil)

	_ object.Service_SearchServer = (*testSearchEntity)(nil)
)

func (s *testSearchEntity) prepareResponse(_ context.Context, req serviceRequest, resp serviceResponse) error {
	if s.f != nil {
		s.f(req, resp)
	}
	return s.err
}

func (s *testSearchEntity) Send(r *object.SearchResponse) error {
	if s.f != nil {
		s.f(r)
	}
	return s.err
}

func (s *testSearchEntity) Context() context.Context { return context.TODO() }

func (s *testSearchEntity) executeOperation(_ context.Context, p transport.MetaInfo, h responseItemHandler) error {
	if s.f != nil {
		s.f(p, h)
	}
	return s.err
}

func (s *testSearchEntity) handleRequest(_ context.Context, p handleRequestParams) (interface{}, error) {
	if s.f != nil {
		s.f(p)
	}
	return s.res, s.err
}

func TestSearchVerify(t *testing.T) {
	t.Run("KeyNoChildren", func(t *testing.T) {
		var (
			q = v1.Query{
				Filters: []QueryFilter{
					{
						Type: v1.Filter_Exact,
						Name: transport.KeyNoChildren,
					},
				},
			}
			obj = new(Object)
		)
		require.True(t, imposeQuery(q, obj))

		obj.Headers = append(obj.Headers, Header{Value: &object.Header_Link{
			Link: &object.Link{
				Type: object.Link_Child,
			},
		}})
		require.False(t, imposeQuery(q, obj))
	})
}

func Test_coreObjAddrSet(t *testing.T) {
	// create address accumulator
	acc := newUniqueAddressAccumulator()
	require.NotNil(t, acc)

	// check type correctness
	v, ok := acc.(*coreObjAddrSet)
	require.True(t, ok)

	// check fields initialization
	require.NotNil(t, v.items)
	require.NotNil(t, v.RWMutex)

	t.Run("add/list", func(t *testing.T) {
		// ascertain that initial list is empty
		require.Empty(t, acc.list())

		// add first set of addresses
		addrList1 := testAddrList(t, 5)
		acc.handleItem(addrList1)

		// ascertain that list is equal to added list
		require.Equal(t, addrList1, acc.list())

		// add more addresses
		addrList2 := testAddrList(t, 5)
		acc.handleItem(addrList2)

		twoLists := append(addrList1, addrList2...)

		// ascertain that list is a concatenation of added lists
		require.Equal(t, twoLists, acc.list())

		// add second list again
		acc.handleItem(addrList2)

		// ascertain that list have not changed after adding existing elements
		require.Equal(t, twoLists, acc.list())
	})
}

func TestObjectService_Search(t *testing.T) {
	req := &object.SearchRequest{
		ContainerID: testObjectAddress(t).CID,
		Query:       testData(t, 10),
	}

	addrList := testAddrList(t, int(addrPerMsg)+5)

	t.Run("request handler failure", func(t *testing.T) {
		rhErr := internal.Error("test error for request handler")
		s := &objectService{
			statusCalculator: newStatusCalculator(),
		}

		s.requestHandler = &testSearchEntity{
			f: func(items ...interface{}) {
				p := items[0].(handleRequestParams)
				require.Equal(t, req, p.request)
				require.Equal(t, s, p.executor)
			},
			err: rhErr,
		}

		require.EqualError(t, s.Search(req, new(testSearchEntity)), rhErr.Error())
	})

	t.Run("server error", func(t *testing.T) {
		srvErr := internal.Error("test error for search server")

		resp := &object.SearchResponse{Addresses: addrList[:addrPerMsg]}

		s := &objectService{
			requestHandler: &testSearchEntity{
				res: addrList,
			},
			respPreparer: &testSearchEntity{
				f: func(items ...interface{}) {
					require.Equal(t, req, items[0])
					require.Equal(t, makeSearchResponse(addrList[:addrPerMsg]), items[1])
				},
				res: resp,
			},

			statusCalculator: newStatusCalculator(),
		}

		srv := &testSearchEntity{
			f: func(items ...interface{}) {
				require.Equal(t, resp, items[0])
			},
			err: srvErr, // force server to return srvErr
		}

		require.EqualError(t, s.Search(req, srv), srvErr.Error())
	})

	t.Run("correct result", func(t *testing.T) {
		handler := &testSearchEntity{res: make([]Address, 0)}

		off := 0

		var resp *object.SearchResponse

		s := &objectService{
			requestHandler: handler,
			respPreparer: &testSearchEntity{
				f: func(items ...interface{}) {
					require.Equal(t, req, items[0])
					resp = items[1].(*object.SearchResponse)
					sz := len(resp.GetAddresses())
					require.Equal(t, makeSearchResponse(addrList[off:off+sz]), items[1])
					off += sz
				},
			},

			statusCalculator: newStatusCalculator(),
		}

		srv := &testSearchEntity{
			f: func(items ...interface{}) {
				require.Equal(t, resp, items[0])
			},
		}

		require.NoError(t, s.Search(req, srv))

		handler.res = addrList

		require.NoError(t, s.Search(req, srv))
	})
}

func Test_coreObjectSearcher(t *testing.T) {
	ctx := context.TODO()

	req := newRawSearchInfo()
	req.setQuery(testData(t, 10))

	t.Run("operation executor failure", func(t *testing.T) {
		execErr := internal.Error("test error for operation executor")

		s := &coreObjectSearcher{
			executor: &testSearchEntity{
				f: func(items ...interface{}) {
					require.Equal(t, req, items[0])
					require.Equal(t, newUniqueAddressAccumulator(), items[1])
				},
				err: execErr,
			},
		}

		res, err := s.searchObjects(ctx, req)
		require.EqualError(t, err, execErr.Error())
		require.Empty(t, res)
	})

	t.Run("correct result", func(t *testing.T) {
		addrList := testAddrList(t, 5)

		s := &coreObjectSearcher{
			executor: &testSearchEntity{
				f: func(items ...interface{}) {
					items[1].(responseItemHandler).handleItem(addrList)
				},
			},
		}

		res, err := s.searchObjects(ctx, req)
		require.NoError(t, err)
		require.Equal(t, addrList, res)
	})
}
