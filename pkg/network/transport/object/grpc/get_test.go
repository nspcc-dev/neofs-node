package object

import (
	"context"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testGetEntity struct {
		// Set of interfaces which entity must implement, but some methods from those does not call.
		localstore.Localstore
		object.Service_GetServer

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var (
	_ object.Service_GetServer = (*testGetEntity)(nil)
	_ requestHandler           = (*testGetEntity)(nil)
	_ responsePreparer         = (*testGetEntity)(nil)
)

func (s *testGetEntity) prepareResponse(_ context.Context, req serviceRequest, resp serviceResponse) error {
	if s.f != nil {
		s.f(req, resp)
	}
	return s.err
}

func (s *testGetEntity) Context() context.Context { return context.TODO() }

func (s *testGetEntity) Send(r *object.GetResponse) error {
	if s.f != nil {
		s.f(r)
	}
	return s.err
}

func (s *testGetEntity) handleRequest(_ context.Context, p handleRequestParams) (interface{}, error) {
	if s.f != nil {
		s.f(p)
	}
	return s.res, s.err
}

func Test_makeGetHeaderResponse(t *testing.T) {
	obj := &Object{Payload: testData(t, 10)}

	require.Equal(t, &object.GetResponse{R: &object.GetResponse_Object{Object: obj}}, makeGetHeaderResponse(obj))
}

func Test_makeGetChunkResponse(t *testing.T) {
	chunk := testData(t, 10)

	require.Equal(t, &object.GetResponse{R: &object.GetResponse_Chunk{Chunk: chunk}}, makeGetChunkResponse(chunk))
}

func Test_splitBytes(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		testSplit(t, make([]byte, 0), 0)
		testSplit(t, nil, 0)
	})

	t.Run("less size", func(t *testing.T) {
		testSplit(t, make([]byte, 10), 20)
	})

	t.Run("equal size", func(t *testing.T) {
		testSplit(t, make([]byte, 20), 20)
	})

	t.Run("oversize", func(t *testing.T) {
		testSplit(t, make([]byte, 3), 17)
	})
}

func testSplit(t *testing.T, initData []byte, maxSize int) {
	res := splitBytes(initData, maxSize)
	restored := make([]byte, 0, len(initData))
	for i := range res {
		require.LessOrEqual(t, len(res[i]), maxSize)
		restored = append(restored, res[i]...)
	}
	require.Len(t, restored, len(initData))
	if len(initData) > 0 {
		require.Equal(t, initData, restored)
	}
}

func TestObjectService_Get(t *testing.T) {
	req := &object.GetRequest{Address: testObjectAddress(t)}

	t.Run("request handler failure", func(t *testing.T) {
		hErr := errors.New("test error for request handler")

		s := &objectService{
			statusCalculator: newStatusCalculator(),
		}

		s.requestHandler = &testGetEntity{
			f: func(items ...interface{}) {
				t.Run("correct request handler params", func(t *testing.T) {
					p := items[0].(handleRequestParams)
					require.Equal(t, req, p.request)
					require.Equal(t, s, p.executor)
				})
			},
			err: hErr,
		}

		require.EqualError(t, s.Get(req, new(testGetEntity)), hErr.Error())
	})

	t.Run("send object head failure", func(t *testing.T) {
		srvErr := errors.New("test error for get server")

		obj := &Object{
			SystemHeader: SystemHeader{
				ID:  testObjectAddress(t).ObjectID,
				CID: testObjectAddress(t).CID,
			},
		}

		s := objectService{
			requestHandler: &testGetEntity{res: &objectData{Object: obj}},
			respPreparer: &testGetEntity{
				f: func(items ...interface{}) {
					require.Equal(t, req, items[0])
					require.Equal(t, makeGetHeaderResponse(obj), items[1])
				},
				res: new(object.GetResponse),
			},

			statusCalculator: newStatusCalculator(),
		}

		require.EqualError(t, s.Get(req, &testGetEntity{err: srvErr}), errors.Wrap(srvErr, emSendObjectHead).Error())
	})

	t.Run("send chunk failure", func(t *testing.T) {
		srvErr := errors.New("test error for get server")
		payload := testData(t, 10)

		obj := &Object{
			SystemHeader: SystemHeader{ID: testObjectAddress(t).ObjectID},
			Headers: []Header{{
				Value: &object.Header_UserHeader{UserHeader: &UserHeader{Key: "key", Value: "value"}},
			}},
			Payload: payload,
		}

		headResp := makeGetHeaderResponse(&Object{
			SystemHeader: obj.SystemHeader,
			Headers:      obj.Headers,
		})

		chunkResp := makeGetChunkResponse(payload)

		callNum := 0

		respPrep := new(testGetEntity)
		respPrep.f = func(items ...interface{}) {
			if callNum == 0 {
				respPrep.res = headResp
			} else {
				respPrep.res = chunkResp
			}
		}

		s := objectService{
			requestHandler: &testGetEntity{res: &objectData{Object: obj}},
			respPreparer:   respPrep,

			getChunkPreparer: respPrep,

			statusCalculator: newStatusCalculator(),
		}

		srv := new(testGetEntity)
		srv.f = func(items ...interface{}) {
			t.Run("correct get server params", func(t *testing.T) {
				if callNum == 0 {
					require.Equal(t, headResp, items[0])
				} else {
					require.Equal(t, chunkResp, items[0])
					srv.err = srvErr
				}
				callNum++
			})
		}

		require.EqualError(t, s.Get(req, srv), srvErr.Error())
	})

	t.Run("send success", func(t *testing.T) {
		s := objectService{
			requestHandler: &testGetEntity{res: &objectData{
				Object:  new(Object),
				payload: new(emptyReader),
			}},
			respPreparer: &testGetEntity{
				res: new(object.GetResponse),
			},

			statusCalculator: newStatusCalculator(),
		}

		require.NoError(t, s.Get(req, new(testGetEntity)))
	})
}
