package object

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testHandlerEntity struct {
		// Set of interfaces which testCommonEntity must implement, but some methods from those does not call.
		serviceRequest
		object.Service_PutServer

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var (
	_ requestPreProcessor      = (*testHandlerEntity)(nil)
	_ requestPostProcessor     = (*testHandlerEntity)(nil)
	_ requestHandleExecutor    = (*testHandlerEntity)(nil)
	_ objectSearcher           = (*testHandlerEntity)(nil)
	_ objectStorer             = (*testHandlerEntity)(nil)
	_ object.Service_PutServer = (*testHandlerEntity)(nil)
	_ objectRemover            = (*testHandlerEntity)(nil)
	_ objectReceiver           = (*testHandlerEntity)(nil)
	_ objectRangeReceiver      = (*testHandlerEntity)(nil)
	_ payloadRangeReceiver     = (*testHandlerEntity)(nil)
	_ responsePreparer         = (*testHandlerEntity)(nil)
)

func (s *testHandlerEntity) prepareResponse(_ context.Context, req serviceRequest, resp serviceResponse) error {
	if s.f != nil {
		s.f(req, resp)
	}
	return s.err
}

func (s *testHandlerEntity) getRangeData(_ context.Context, info transport.RangeInfo, l ...Object) (io.Reader, error) {
	if s.f != nil {
		s.f(info, l)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(io.Reader), nil
}

func (s *testHandlerEntity) getRange(_ context.Context, r rangeTool) (interface{}, error) {
	if s.f != nil {
		s.f(r)
	}
	return s.res, s.err
}

func (s *testHandlerEntity) getObject(_ context.Context, r ...transport.GetInfo) (*objectData, error) {
	if s.f != nil {
		s.f(r)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*objectData), nil
}

func (s *testHandlerEntity) delete(_ context.Context, r deleteInfo) error {
	if s.f != nil {
		s.f(r)
	}
	return s.err
}

func (s *testHandlerEntity) SendAndClose(r *object.PutResponse) error {
	if s.f != nil {
		s.f(r)
	}
	return s.err
}

func (s *testHandlerEntity) putObject(_ context.Context, r transport.PutInfo) (*Address, error) {
	if s.f != nil {
		s.f(r)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*Address), nil
}

func (s *testHandlerEntity) searchObjects(_ context.Context, r transport.SearchInfo) ([]Address, error) {
	if s.f != nil {
		s.f(r)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]Address), nil
}

func (s *testHandlerEntity) preProcess(_ context.Context, req serviceRequest) error {
	if s.f != nil {
		s.f(req)
	}
	return s.err
}

func (s *testHandlerEntity) postProcess(_ context.Context, req serviceRequest, e error) {
	if s.f != nil {
		s.f(req, e)
	}
}

func (s *testHandlerEntity) executeRequest(_ context.Context, req serviceRequest) (interface{}, error) {
	if s.f != nil {
		s.f(req)
	}
	return s.res, s.err
}

func TestCoreRequestHandler_HandleRequest(t *testing.T) {
	ctx := context.TODO()

	// create custom serviceRequest
	req := new(testHandlerEntity)

	t.Run("pre processor error", func(t *testing.T) {
		// create custom error
		pErr := errors.New("test error for pre-processor")

		s := &coreRequestHandler{
			preProc: &testHandlerEntity{
				f: func(items ...interface{}) {
					t.Run("correct pre processor params", func(t *testing.T) {
						require.Equal(t, req, items[0].(serviceRequest))
					})
				},
				err: pErr, // force requestPreProcessor to return pErr
			},
		}

		res, err := s.handleRequest(ctx, handleRequestParams{request: req})

		// ascertain that error returns as expected
		require.EqualError(t, err, pErr.Error())

		// ascertain that nil result returns as expected
		require.Nil(t, res)
	})

	t.Run("correct behavior", func(t *testing.T) {
		// create custom error
		eErr := errors.New("test error for request executor")

		// create custom result
		eRes := testData(t, 10)

		// create channel for requestPostProcessor
		ch := make(chan struct{})

		executor := &testHandlerEntity{
			f: func(items ...interface{}) {
				t.Run("correct executor params", func(t *testing.T) {
					require.Equal(t, req, items[0].(serviceRequest))
				})
			},
			res: eRes, // force requestHandleExecutor to return created result
			err: eErr, // force requestHandleExecutor to return created error
		}

		s := &coreRequestHandler{
			preProc: &testHandlerEntity{
				err: nil, // force requestPreProcessor to return nil error
			},
			postProc: &testHandlerEntity{
				f: func(items ...interface{}) {
					t.Run("correct pre processor params", func(t *testing.T) {
						require.Equal(t, req, items[0].(serviceRequest))
						require.Equal(t, eErr, items[1].(error))
					})
					ch <- struct{}{} // write to channel
				},
			},
		}

		res, err := s.handleRequest(ctx, handleRequestParams{
			request:  req,
			executor: executor,
		})

		// ascertain that results return as expected
		require.EqualError(t, err, eErr.Error())
		require.Equal(t, eRes, res)

		<-ch // read from channel
	})
}

func Test_objectService_executeRequest(t *testing.T) {
	ctx := context.TODO()

	t.Run("invalid request", func(t *testing.T) {
		req := new(testHandlerEntity)
		require.PanicsWithValue(t, fmt.Sprintf(pmWrongRequestType, req), func() {
			_, _ = new(objectService).executeRequest(ctx, req)
		})
	})

	t.Run("search request", func(t *testing.T) {
		var (
			timeout  = 3 * time.Second
			req      = &object.SearchRequest{ContainerID: testObjectAddress(t).CID}
			addrList = testAddrList(t, 3)
		)

		s := &objectService{
			pSrch: OperationParams{Timeout: timeout},
			objSearcher: &testHandlerEntity{
				f: func(items ...interface{}) {
					require.Equal(t, &transportRequest{
						serviceRequest: req,
						timeout:        timeout,
					}, items[0])
				},
				res: addrList,
			},
		}

		res, err := s.executeRequest(ctx, req)
		require.NoError(t, err)
		require.Equal(t, addrList, res)
	})

	t.Run("put request", func(t *testing.T) {
		t.Run("storer error", func(t *testing.T) {
			sErr := errors.New("test error for object storer")

			req := &putRequest{
				PutRequest: new(object.PutRequest),
				srv:        new(testHandlerEntity),
				timeout:    3 * time.Second,
			}

			s := &objectService{
				objStorer: &testHandlerEntity{
					f: func(items ...interface{}) {
						require.Equal(t, req, items[0])
					},
					err: sErr,
				},
				respPreparer: &testHandlerEntity{
					res: serviceResponse(nil),
				},
			}

			_, err := s.executeRequest(ctx, req)
			require.EqualError(t, err, sErr.Error())
		})

		t.Run("correct result", func(t *testing.T) {
			addr := testObjectAddress(t)

			srvErr := errors.New("test error for stream server")

			resp := &object.PutResponse{Address: addr}

			pReq := new(object.PutRequest)

			s := &objectService{
				objStorer: &testHandlerEntity{
					res: &addr,
				},
				respPreparer: &testHandlerEntity{
					f: func(items ...interface{}) {
						require.Equal(t, pReq, items[0])
						require.Equal(t, makePutResponse(addr), items[1])
					},
					res: resp,
				},
			}

			req := &putRequest{
				PutRequest: pReq,
				srv: &testHandlerEntity{
					f: func(items ...interface{}) {
						require.Equal(t, resp, items[0])
					},
					err: srvErr,
				},
			}

			res, err := s.executeRequest(ctx, req)
			require.EqualError(t, err, srvErr.Error())
			require.Nil(t, res)
		})
	})

	t.Run("delete request", func(t *testing.T) {
		var (
			timeout = 3 * time.Second
			dErr    = errors.New("test error for object remover")
			req     = &object.DeleteRequest{Address: testObjectAddress(t)}
		)

		s := &objectService{
			objRemover: &testHandlerEntity{
				f: func(items ...interface{}) {
					require.Equal(t, &transportRequest{
						serviceRequest: req,
						timeout:        timeout,
					}, items[0])
				},
				err: dErr,
			},
			pDel: OperationParams{Timeout: timeout},
		}

		res, err := s.executeRequest(ctx, req)
		require.EqualError(t, err, dErr.Error())
		require.Nil(t, res)
	})

	t.Run("get request", func(t *testing.T) {
		var (
			timeout = 3 * time.Second
			obj     = &objectData{Object: &Object{Payload: testData(t, 10)}}
			req     = &object.GetRequest{Address: testObjectAddress(t)}
		)

		s := &objectService{
			objRecv: &testHandlerEntity{
				f: func(items ...interface{}) {
					require.Equal(t, []transport.GetInfo{&transportRequest{
						serviceRequest: req,
						timeout:        timeout,
					}}, items[0])
				},
				res: obj,
			},
			pGet: OperationParams{Timeout: timeout},
		}

		res, err := s.executeRequest(ctx, req)
		require.NoError(t, err)
		require.Equal(t, obj, res)
	})

	t.Run("head request", func(t *testing.T) {
		var (
			timeout = 3 * time.Second
			hErr    = errors.New("test error for head receiver")
			req     = &object.HeadRequest{Address: testObjectAddress(t)}
		)

		s := &objectService{
			objRecv: &testHandlerEntity{
				f: func(items ...interface{}) {
					require.Equal(t, []transport.GetInfo{&transportRequest{
						serviceRequest: req,
						timeout:        timeout,
					}}, items[0])
				},
				err: hErr,
			},
			pHead: OperationParams{Timeout: timeout},
		}

		_, err := s.executeRequest(ctx, req)
		require.EqualError(t, err, hErr.Error())
	})

	t.Run("range requests", func(t *testing.T) {
		t.Run("data", func(t *testing.T) {
			var (
				timeout = 3 * time.Second
				rData   = testData(t, 10)
				req     = &GetRangeRequest{Address: testObjectAddress(t)}
			)

			s := &objectService{
				payloadRngRecv: &testHandlerEntity{
					f: func(items ...interface{}) {
						require.Equal(t, &transportRequest{
							serviceRequest: req,
							timeout:        timeout,
						}, items[0])
						require.Empty(t, items[1])
					},
					res: bytes.NewReader(rData),
				},
				pRng: OperationParams{Timeout: timeout},
			}

			res, err := s.executeRequest(ctx, req)
			require.NoError(t, err)
			d, err := ioutil.ReadAll(res.(io.Reader))
			require.NoError(t, err)
			require.Equal(t, rData, d)
		})

		t.Run("hashes", func(t *testing.T) {
			var (
				timeout = 3 * time.Second
				rErr    = errors.New("test error for range receiver")
				req     = &object.GetRangeHashRequest{Address: testObjectAddress(t)}
			)

			s := &objectService{
				rngRecv: &testHandlerEntity{
					f: func(items ...interface{}) {
						require.Equal(t, &transportRequest{
							serviceRequest: req,
							timeout:        timeout,
						}, items[0])
					},
					err: rErr,
				},
				pRng: OperationParams{Timeout: timeout},
			}

			_, err := s.executeRequest(ctx, req)
			require.EqualError(t, err, rErr.Error())
		})
	})
}
