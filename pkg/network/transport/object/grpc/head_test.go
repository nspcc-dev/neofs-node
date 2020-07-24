package object

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testHeadEntity struct {
		// Set of interfaces which entity must implement, but some methods from those does not call.
		transformer.ObjectRestorer

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var (
	_ ancestralObjectsReceiver   = (*testHeadEntity)(nil)
	_ objectChildrenLister       = (*testHeadEntity)(nil)
	_ objectReceiver             = (*testHeadEntity)(nil)
	_ requestHandler             = (*testHeadEntity)(nil)
	_ operationExecutor          = (*testHeadEntity)(nil)
	_ objectRewinder             = (*testHeadEntity)(nil)
	_ transformer.ObjectRestorer = (*testHeadEntity)(nil)
	_ responsePreparer           = (*testHeadEntity)(nil)
)

func (s *testHeadEntity) prepareResponse(_ context.Context, req serviceRequest, resp serviceResponse) error {
	if s.f != nil {
		s.f(req, resp)
	}
	return s.err
}

func (s *testHeadEntity) getFromChildren(ctx context.Context, addr Address, ids []ID, h bool) (*objectData, error) {
	if s.f != nil {
		s.f(addr, ids, h, ctx)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*objectData), nil
}

func (s *testHeadEntity) Restore(_ context.Context, objs ...Object) ([]Object, error) {
	if s.f != nil {
		s.f(objs)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]Object), nil
}

func (s *testHeadEntity) rewind(ctx context.Context, objs ...Object) (*Object, error) {
	if s.f != nil {
		s.f(objs)
	}
	return s.res.(*Object), s.err
}

func (s *testHeadEntity) executeOperation(_ context.Context, i transport.MetaInfo, h responseItemHandler) error {
	if s.f != nil {
		s.f(i, h)
	}
	return s.err
}

func (s *testHeadEntity) children(ctx context.Context, addr Address) []ID {
	if s.f != nil {
		s.f(addr, ctx)
	}
	return s.res.([]ID)
}

func (s *testHeadEntity) getObject(_ context.Context, p ...transport.GetInfo) (*objectData, error) {
	if s.f != nil {
		s.f(p)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*objectData), nil
}

func (s *testHeadEntity) handleRequest(_ context.Context, p handleRequestParams) (interface{}, error) {
	if s.f != nil {
		s.f(p)
	}
	return s.res, s.err
}

func Test_transportRequest_HeadInfo(t *testing.T) {
	t.Run("address", func(t *testing.T) {
		t.Run("valid request", func(t *testing.T) {
			addr := testObjectAddress(t)

			reqs := []transportRequest{
				{serviceRequest: &object.HeadRequest{Address: addr}},
				{serviceRequest: &object.GetRequest{Address: addr}},
				{serviceRequest: &GetRangeRequest{Address: addr}},
				{serviceRequest: &object.GetRangeHashRequest{Address: addr}},
				{serviceRequest: &object.DeleteRequest{Address: addr}},
			}

			for i := range reqs {
				require.Equal(t, addr, reqs[i].GetAddress())
			}
		})

		t.Run("unknown request", func(t *testing.T) {
			req := new(object.SearchRequest)

			r := &transportRequest{
				serviceRequest: req,
			}

			require.PanicsWithValue(t, fmt.Sprintf(pmWrongRequestType, req), func() {
				_ = r.GetAddress()
			})
		})
	})

	t.Run("full headers", func(t *testing.T) {
		r := &transportRequest{
			serviceRequest: &object.HeadRequest{
				FullHeaders: true,
			},
		}

		require.True(t, r.GetFullHeaders())
	})

	t.Run("raw", func(t *testing.T) {
		hReq := new(object.HeadRequest)
		hReq.SetRaw(true)

		r := &transportRequest{
			serviceRequest: hReq,
		}
		require.True(t, r.Raw())

		hReq.SetRaw(false)
		require.False(t, r.Raw())
	})
}

func Test_rawHeadInfo(t *testing.T) {
	t.Run("address", func(t *testing.T) {
		addr := testObjectAddress(t)

		r := newRawHeadInfo()
		r.setAddress(addr)

		require.Equal(t, addr, r.GetAddress())
	})

	t.Run("full headers", func(t *testing.T) {
		r := newRawHeadInfo()
		r.setFullHeaders(true)

		require.True(t, r.GetFullHeaders())
	})
}

func Test_coreObjAccum(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		s := newObjectAccumulator()
		v := s.(*coreObjAccum)
		require.Nil(t, v.obj)
		require.NotNil(t, v.Once)
	})

	t.Run("handle/object", func(t *testing.T) {
		obj1 := new(Object)

		s := newObjectAccumulator()

		// add first object
		s.handleItem(obj1)

		// ascertain tha object was added
		require.Equal(t, obj1, s.object())

		obj2 := new(Object)

		// add second object
		s.handleItem(obj2)

		// ascertain that second object was ignored
		require.Equal(t, obj1, s.object())
	})
}

func Test_objectService_Head(t *testing.T) {
	ctx := context.TODO()

	t.Run("request handler error", func(t *testing.T) {
		// create custom error for test
		rhErr := errors.New("test error for request handler")

		// create custom request for test
		req := new(object.HeadRequest)

		s := &objectService{
			statusCalculator: newStatusCalculator(),
		}

		s.requestHandler = &testHeadEntity{
			f: func(items ...interface{}) {
				t.Run("correct request handler params", func(t *testing.T) {
					p := items[0].(handleRequestParams)
					require.Equal(t, s, p.executor)
					require.Equal(t, req, p.request)
				})
			},
			err: rhErr, // force requestHandler to return rhErr
		}

		res, err := s.Head(ctx, req)
		require.EqualError(t, err, rhErr.Error())
		require.Nil(t, res)
	})

	t.Run("correct resulst", func(t *testing.T) {
		obj := &objectData{Object: new(Object)}

		resp := &object.HeadResponse{Object: obj.Object}

		req := new(object.HeadRequest)

		s := &objectService{
			requestHandler: &testHeadEntity{
				res: obj, // force request handler to return obj
			},
			respPreparer: &testHeadEntity{
				f: func(items ...interface{}) {
					require.Equal(t, req, items[0])
					require.Equal(t, makeHeadResponse(obj.Object), items[1])
				},
				res: resp,
			},

			statusCalculator: newStatusCalculator(),
		}

		res, err := s.Head(ctx, new(object.HeadRequest))
		require.NoError(t, err)
		require.Equal(t, resp, res)
	})
}

func Test_coreHeadReceiver_head(t *testing.T) {
	ctx := context.TODO()

	t.Run("raw handling", func(t *testing.T) {
		// create custom head info for test
		hInfo := newRawHeadInfo()
		hInfo.setRaw(true)

		// create custom error for test
		srErr := errors.New("test error for straight object receiver")

		s := &coreObjectReceiver{
			straightObjRecv: &testHeadEntity{
				err: srErr, // force straightObjectReceiver to return srErr
			},
		}

		_, err := s.getObject(ctx, hInfo)
		// ascertain that straightObjectReceiver result returns in raw case as expected
		require.EqualError(t, err, srErr.Error())
	})

	t.Run("straight receive of non-linking object", func(t *testing.T) {
		// create custom head info for test
		hInfo := newRawHeadInfo()

		// create object w/o children for test
		obj := &objectData{Object: new(Object)}

		s := &coreObjectReceiver{
			straightObjRecv: &testHeadEntity{
				f: func(items ...interface{}) {
					t.Run("correct straight receiver params", func(t *testing.T) {
						require.Equal(t, []transport.GetInfo{hInfo}, items[0])
					})
				},
				res: obj,
			},
		}

		res, err := s.getObject(ctx, hInfo)
		require.NoError(t, err)
		require.Equal(t, obj, res)
	})

	t.Run("linking object/non-assembly", func(t *testing.T) {
		// create custom head info for test
		hInfo := newRawHeadInfo()

		// create object w/ children for test
		obj := &objectData{
			Object: &Object{Headers: []Header{{Value: &object.Header_Link{Link: &object.Link{Type: object.Link_Child}}}}},
		}

		s := &coreObjectReceiver{
			straightObjRecv: &testHeadEntity{
				res: obj, // force straightObjectReceiver to return obj
			},
			ancestralRecv: nil, // make component to be non-assembly
		}

		res, err := s.getObject(ctx, hInfo)
		require.EqualError(t, err, errNonAssembly.Error())
		require.Nil(t, res)
	})

	t.Run("children search failure", func(t *testing.T) {
		addr := testObjectAddress(t)

		hInfo := newRawHeadInfo()
		hInfo.setAddress(addr)
		hInfo.setSessionToken(new(service.Token))

		s := &coreObjectReceiver{
			straightObjRecv: &testHeadEntity{
				err: errors.New(""), // force straightObjectReceiver to return non-empty error
			},
			childLister: &testHeadEntity{
				f: func(items ...interface{}) {
					t.Run("correct child lister params", func(t *testing.T) {
						require.Equal(t, addr, items[0])
						require.Equal(t,
							hInfo.GetSessionToken(),
							items[1].(context.Context).Value(transformer.PublicSessionToken),
						)
					})
				},
				res: make([]ID, 0), // force objectChildren lister to return empty list
			},
			ancestralRecv: new(testHeadEntity),
		}

		res, err := s.getObject(ctx, hInfo)
		require.EqualError(t, err, childrenNotFound.Error())
		require.Nil(t, res)
	})

	t.Run("correct result", func(t *testing.T) {
		var (
			childCount = 5
			rErr       = errors.New("test error for rewinding receiver")
			children   = make([]ID, 0, childCount)
		)

		for i := 0; i < childCount; i++ {
			id := testObjectAddress(t).ObjectID
			children = append(children, id)
		}

		// create custom head info
		hInfo := newRawHeadInfo()
		hInfo.setTTL(5)
		hInfo.setTimeout(3 * time.Second)
		hInfo.setAddress(testObjectAddress(t))
		hInfo.setSessionToken(new(service.Token))

		t.Run("error/children from straight receiver", func(t *testing.T) {
			obj := &objectData{Object: new(Object)}

			for i := range children {
				// add child reference to object
				obj.Headers = append(obj.Headers, Header{
					Value: &object.Header_Link{Link: &object.Link{Type: object.Link_Child, ID: children[i]}},
				})
			}

			s := &coreObjectReceiver{
				straightObjRecv: &testHeadEntity{
					res: obj, // force straight receiver to return obj
				},
				ancestralRecv: &testHeadEntity{
					f: func(items ...interface{}) {
						t.Run("correct rewinding receiver", func(t *testing.T) {
							require.Equal(t, hInfo.GetAddress(), items[0])
							require.Equal(t, children, items[1])
							require.True(t, items[2].(bool))
							require.Equal(t,
								hInfo.GetSessionToken(),
								items[3].(context.Context).Value(transformer.PublicSessionToken),
							)
						})
					},
					err: rErr, // force rewinding receiver to return rErr
				},
				log: zap.L(),
			}

			res, err := s.getObject(ctx, hInfo)
			require.EqualError(t, err, errIncompleteOperation.Error())
			require.Nil(t, res)
		})

		t.Run("success/children from child lister", func(t *testing.T) {
			obj := &objectData{Object: new(Object)}

			s := &coreObjectReceiver{
				straightObjRecv: &testHeadEntity{
					err: errors.New(""), // force straight receiver to return non-nil error
				},
				ancestralRecv: &testHeadEntity{
					f: func(items ...interface{}) {
						t.Run("correct rewinding receiver", func(t *testing.T) {
							require.Equal(t, hInfo.GetAddress(), items[0])
							require.Equal(t, children, items[1])
							require.True(t, items[2].(bool))
						})
					},
					res: obj, // force rewinding receiver to return obj
				},
				childLister: &testHeadEntity{
					res: children, // force objectChildrenLister to return particular list
				},
			}

			res, err := s.getObject(ctx, hInfo)
			require.NoError(t, err, rErr.Error())
			require.Equal(t, obj, res)
		})
	})
}

func Test_straightHeadReceiver_head(t *testing.T) {
	ctx := context.TODO()

	hInfo := newRawHeadInfo()
	hInfo.setFullHeaders(true)

	t.Run("executor error", func(t *testing.T) {
		exErr := errors.New("test error for operation executor")

		s := &straightObjectReceiver{
			executor: &testHeadEntity{
				f: func(items ...interface{}) {
					t.Run("correct operation executor params", func(t *testing.T) {
						require.Equal(t, hInfo, items[0])
						_ = items[1].(objectAccumulator)
					})
				},
				err: exErr, // force operationExecutor to return exErr
			},
		}

		_, err := s.getObject(ctx, hInfo)
		require.EqualError(t, err, exErr.Error())

		hInfo = newRawHeadInfo()
		hInfo.setFullHeaders(true)

		_, err = s.getObject(ctx, hInfo)
		require.EqualError(t, err, exErr.Error())
	})

	t.Run("correct result", func(t *testing.T) {
		obj := &objectData{Object: new(Object), payload: new(emptyReader)}

		s := &straightObjectReceiver{
			executor: &testHeadEntity{
				f: func(items ...interface{}) {
					items[1].(objectAccumulator).handleItem(obj.Object)
				},
			},
		}

		res, err := s.getObject(ctx, hInfo)
		require.NoError(t, err)
		require.Equal(t, obj, res)
	})
}

func Test_coreObjectRewinder_rewind(t *testing.T) {
	ctx := context.TODO()

	t.Run("transformer failure", func(t *testing.T) {
		tErr := errors.New("test error for object transformer")
		objs := []Object{*new(Object), *new(Object)}

		s := &coreObjectRewinder{
			transformer: &testHeadEntity{
				f: func(items ...interface{}) {
					t.Run("correct transformer params", func(t *testing.T) {
						require.Equal(t, objs, items[0])
					})
				},
				err: tErr, // force transformer to return tErr
			},
		}

		res, err := s.rewind(ctx, objs...)
		require.EqualError(t, err, tErr.Error())
		require.Empty(t, res)
	})

	t.Run("correct result", func(t *testing.T) {
		objs := []Object{
			{SystemHeader: SystemHeader{ID: testObjectAddress(t).ObjectID}},
			{SystemHeader: SystemHeader{ID: testObjectAddress(t).ObjectID}},
		}

		s := &coreObjectRewinder{
			transformer: &testHeadEntity{
				res: objs, // force transformer to return objs
			},
		}

		res, err := s.rewind(ctx, objs...)
		require.NoError(t, err)
		require.Equal(t, &objs[0], res)
	})
}

func Test_coreObjectReceiver_sendingRequest(t *testing.T) {
	t.Run("non-assembly", func(t *testing.T) {
		src := &transportRequest{serviceRequest: new(object.GetRequest)}
		// ascertain that request not changed if node is non-assembled
		require.Equal(t, src, new(coreObjectReceiver).sendingRequest(src))
	})

	t.Run("assembly", func(t *testing.T) {
		s := &coreObjectReceiver{ancestralRecv: new(testHeadEntity)}

		t.Run("raw request", func(t *testing.T) {
			src := newRawGetInfo()
			src.setRaw(true)
			// ascertain that request not changed if request is raw
			require.Equal(t, src, s.sendingRequest(src))
		})

		t.Run("non-raw request", func(t *testing.T) {
			getInfo := *newRawGetInfo()
			getInfo.setTTL(uint32(5))
			getInfo.setTimeout(3 * time.Second)
			getInfo.setAddress(testObjectAddress(t))
			getInfo.setRaw(false)
			getInfo.setSessionToken(new(service.Token))

			t.Run("get", func(t *testing.T) {
				res := s.sendingRequest(getInfo)
				require.Equal(t, getInfo.GetTimeout(), res.GetTimeout())
				require.Equal(t, getInfo.GetAddress(), res.GetAddress())
				require.Equal(t, getInfo.GetTTL(), res.GetTTL())
				require.Equal(t, getInfo.GetSessionToken(), res.GetSessionToken())
				require.True(t, res.GetRaw())

				t.Run("zero ttl", func(t *testing.T) {
					res := s.sendingRequest(newRawGetInfo())
					require.Equal(t, uint32(service.NonForwardingTTL), res.GetTTL())
				})
			})

			t.Run("head", func(t *testing.T) {
				hInfo := newRawHeadInfo()
				hInfo.setGetInfo(getInfo)
				hInfo.setFullHeaders(false)

				res := s.sendingRequest(hInfo)
				require.Equal(t, getInfo.GetTimeout(), res.GetTimeout())
				require.Equal(t, getInfo.GetAddress(), res.GetAddress())
				require.Equal(t, getInfo.GetTTL(), res.GetTTL())
				require.Equal(t, getInfo.GetSessionToken(), res.GetSessionToken())
				require.True(t, res.GetRaw())
				require.True(t, res.(transport.HeadInfo).GetFullHeaders())
			})
		})
	})
}
