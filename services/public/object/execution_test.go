package object

import (
	"context"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testExecutionEntity struct {
		// Set of interfaces which testExecutionEntity must implement, but some methods from those does not call.
		transport.MetaInfo
		localstore.Localstore
		containerTraverser

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

func (s *testExecutionEntity) HandleResult(_ context.Context, n multiaddr.Multiaddr, r interface{}, e error) {
	if s.f != nil {
		s.f(n, r, e)
	}
}

var (
	_ transport.ResultHandler                   = (*testExecutionEntity)(nil)
	_ interceptorPreparer                       = (*testExecutionEntity)(nil)
	_ implementations.ContainerTraverseExecutor = (*testExecutionEntity)(nil)
	_ WorkerPool                                = (*testExecutionEntity)(nil)
	_ operationExecutor                         = (*testExecutionEntity)(nil)
	_ placementBuilder                          = (*testExecutionEntity)(nil)
	_ implementations.AddressStore              = (*testExecutionEntity)(nil)
	_ executionParamsComputer                   = (*testExecutionEntity)(nil)
	_ operationFinalizer                        = (*testExecutionEntity)(nil)
	_ EpochReceiver                             = (*testExecutionEntity)(nil)
	_ localstore.Localstore                     = (*testExecutionEntity)(nil)
	_ containerTraverser                        = (*testExecutionEntity)(nil)
	_ responseItemHandler                       = (*testExecutionEntity)(nil)
	_ resultTracker                             = (*testExecutionEntity)(nil)
	_ localObjectStorer                         = (*testExecutionEntity)(nil)
	_ localFullObjectReceiver                   = (*testExecutionEntity)(nil)
	_ localHeadReceiver                         = (*testExecutionEntity)(nil)
	_ localQueryImposer                         = (*testExecutionEntity)(nil)
	_ localRangeReader                          = (*testExecutionEntity)(nil)
	_ localRangeHasher                          = (*testExecutionEntity)(nil)
)

func (s *testExecutionEntity) prepareInterceptor(p interceptorItems) (func(context.Context, multiaddr.Multiaddr) bool, error) {
	if s.f != nil {
		s.f(p)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(func(context.Context, multiaddr.Multiaddr) bool), nil
}

func (s *testExecutionEntity) Execute(_ context.Context, p implementations.TraverseParams) {
	if s.f != nil {
		s.f(p)
	}
}

func (s *testExecutionEntity) Submit(func()) error {
	return s.err
}

func (s *testExecutionEntity) executeOperation(ctx context.Context, r transport.MetaInfo, h responseItemHandler) error {
	if s.f != nil {
		s.f(r, h)
	}
	return s.err
}

func (s *testExecutionEntity) buildPlacement(_ context.Context, a Address, n ...multiaddr.Multiaddr) ([]multiaddr.Multiaddr, error) {
	if s.f != nil {
		s.f(a, n)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]multiaddr.Multiaddr), nil
}

func (s *testExecutionEntity) getHashes(_ context.Context, a Address, r []Range, sa []byte) ([]Hash, error) {
	if s.f != nil {
		s.f(a, r, sa)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]Hash), nil
}

func (s *testExecutionEntity) getRange(_ context.Context, addr Address, rngs Range) ([]byte, error) {
	if s.f != nil {
		s.f(addr, rngs)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]byte), nil
}

func (s *testExecutionEntity) imposeQuery(_ context.Context, c CID, d []byte, v int) ([]Address, error) {
	if s.f != nil {
		s.f(c, d, v)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]Address), nil
}

func (s *testExecutionEntity) headObject(_ context.Context, addr Address) (*Object, error) {
	if s.f != nil {
		s.f(addr)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*Object), nil
}

func (s *testExecutionEntity) getObject(_ context.Context, addr Address) (*Object, error) {
	if s.f != nil {
		s.f(addr)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*Object), nil
}

func (s *testExecutionEntity) putObject(_ context.Context, obj *Object) error {
	if s.f != nil {
		s.f(obj)
	}
	return s.err
}

func (s *testExecutionEntity) trackResult(_ context.Context, p resultItems) {
	if s.f != nil {
		s.f(p)
	}
}

func (s *testExecutionEntity) handleItem(v interface{}) {
	if s.f != nil {
		s.f(v)
	}
}

func (s *testExecutionEntity) add(n multiaddr.Multiaddr, b bool) {
	if s.f != nil {
		s.f(n, b)
	}
}

func (s *testExecutionEntity) done(n multiaddr.Multiaddr) bool {
	if s.f != nil {
		s.f(n)
	}
	return s.res.(bool)
}

func (s *testExecutionEntity) close() {
	if s.f != nil {
		s.f()
	}
}

func (s *testExecutionEntity) PRead(ctx context.Context, addr Address, rng Range) ([]byte, error) {
	if s.f != nil {
		s.f(addr, rng)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]byte), nil
}

func (s *testExecutionEntity) Put(ctx context.Context, obj *Object) error {
	if s.f != nil {
		s.f(ctx, obj)
	}
	return s.err
}

func (s *testExecutionEntity) Get(addr Address) (*Object, error) {
	if s.f != nil {
		s.f(addr)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*Object), nil
}

func (s *testExecutionEntity) Meta(addr Address) (*Meta, error) {
	if s.f != nil {
		s.f(addr)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*Meta), nil
}

func (s *testExecutionEntity) Has(addr Address) (bool, error) {
	if s.f != nil {
		s.f(addr)
	}
	if s.err != nil {
		return false, s.err
	}
	return s.res.(bool), nil
}

func (s *testExecutionEntity) Epoch() uint64 { return s.res.(uint64) }

func (s *testExecutionEntity) completeExecution(_ context.Context, p operationParams) error {
	if s.f != nil {
		s.f(p)
	}
	return s.err
}

func (s *testExecutionEntity) computeParams(p *computableParams, r transport.MetaInfo) {
	if s.f != nil {
		s.f(p, r)
	}
}

func (s *testExecutionEntity) SelfAddr() (multiaddr.Multiaddr, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(multiaddr.Multiaddr), nil
}

func (s *testExecutionEntity) Type() object.RequestType {
	return s.res.(object.RequestType)
}

func Test_typeOfRequest(t *testing.T) {
	t.Run("correct mapping", func(t *testing.T) {
		items := []struct {
			exp object.RequestType
			v   transport.MetaInfo
		}{
			{exp: object.RequestSearch, v: &transportRequest{serviceRequest: new(object.SearchRequest)}},
			{exp: object.RequestSearch, v: newRawSearchInfo()},
			{exp: object.RequestPut, v: new(putRequest)},
			{exp: object.RequestPut, v: &transportRequest{serviceRequest: new(object.PutRequest)}},
			{exp: object.RequestGet, v: newRawGetInfo()},
			{exp: object.RequestGet, v: &transportRequest{serviceRequest: new(object.GetRequest)}},
			{exp: object.RequestHead, v: newRawHeadInfo()},
			{exp: object.RequestHead, v: &transportRequest{serviceRequest: new(object.HeadRequest)}},
			{exp: object.RequestRange, v: newRawRangeInfo()},
			{exp: object.RequestRange, v: &transportRequest{serviceRequest: new(GetRangeRequest)}},
			{exp: object.RequestRangeHash, v: newRawRangeHashInfo()},
			{exp: object.RequestRangeHash, v: &transportRequest{serviceRequest: new(object.GetRangeHashRequest)}},
		}

		for i := range items {
			require.Equal(t, items[i].exp, items[i].v.Type())
		}
	})
}

func Test_coreExecParamsComp_computeParams(t *testing.T) {
	s := new(coreExecParamsComp)
	addr := testObjectAddress(t)

	t.Run("put", func(t *testing.T) {
		addr := testObjectAddress(t)

		p := new(computableParams)
		r := &putRequest{PutRequest: &object.PutRequest{
			R: &object.PutRequest_Header{
				Header: &object.PutRequest_PutHeader{
					Object: &Object{
						SystemHeader: SystemHeader{
							ID:  addr.ObjectID,
							CID: addr.CID,
						},
					},
				},
			},
		}}

		s.computeParams(p, r)

		t.Run("non-forwarding behavior", func(t *testing.T) {
			require.Equal(t, 1, p.stopCount)
		})

		r.SetTTL(service.NonForwardingTTL)

		s.computeParams(p, r)

		require.False(t, p.allowPartialResult)
		require.False(t, p.tryPreviousNetMap)
		require.False(t, p.selfForward)
		require.Equal(t, addr, p.addr)
		require.Equal(t, 0, p.maxRecycleCount)
		require.Equal(t, 0, int(r.CopiesNumber()))
	})

	t.Run("get", func(t *testing.T) {
		p := new(computableParams)

		r := newRawGetInfo()
		r.setAddress(addr)

		s.computeParams(p, r)

		require.Equal(t, 1, p.stopCount)
		require.False(t, p.allowPartialResult)
		require.True(t, p.tryPreviousNetMap)
		require.False(t, p.selfForward)
		require.Equal(t, addr, p.addr)
		require.Equal(t, 0, p.maxRecycleCount)
	})

	t.Run("head", func(t *testing.T) {
		p := new(computableParams)
		r := &transportRequest{serviceRequest: &object.HeadRequest{Address: addr}}

		s.computeParams(p, r)

		require.Equal(t, 1, p.stopCount)
		require.False(t, p.allowPartialResult)
		require.True(t, p.tryPreviousNetMap)
		require.False(t, p.selfForward)
		require.Equal(t, addr, p.addr)
		require.Equal(t, 0, p.maxRecycleCount)
	})

	t.Run("search", func(t *testing.T) {
		p := new(computableParams)
		r := &transportRequest{serviceRequest: &object.SearchRequest{ContainerID: addr.CID}}

		s.computeParams(p, r)

		require.Equal(t, -1, p.stopCount)
		require.True(t, p.allowPartialResult)
		require.True(t, p.tryPreviousNetMap)
		require.False(t, p.selfForward)
		require.Equal(t, addr.CID, p.addr.CID)
		require.True(t, p.addr.ObjectID.Empty())
		require.Equal(t, 0, p.maxRecycleCount)
	})

	t.Run("range", func(t *testing.T) {
		p := new(computableParams)

		r := newRawRangeInfo()
		r.setAddress(addr)

		s.computeParams(p, r)

		require.Equal(t, 1, p.stopCount)
		require.False(t, p.allowPartialResult)
		require.False(t, p.tryPreviousNetMap)
		require.False(t, p.selfForward)
		require.Equal(t, addr, p.addr)
		require.Equal(t, 0, p.maxRecycleCount)
	})

	t.Run("range hash", func(t *testing.T) {
		p := new(computableParams)

		r := newRawRangeHashInfo()
		r.setAddress(addr)

		s.computeParams(p, r)

		require.Equal(t, 1, p.stopCount)
		require.False(t, p.allowPartialResult)
		require.False(t, p.tryPreviousNetMap)
		require.False(t, p.selfForward)
		require.Equal(t, addr, p.addr)
		require.Equal(t, 0, p.maxRecycleCount)
	})
}

func Test_coreOperationExecutor_executeOperation(t *testing.T) {
	ctx := context.TODO()

	t.Run("correct result", func(t *testing.T) {
		t.Run("error", func(t *testing.T) {
			p := new(testExecutionEntity)
			req := newRawPutInfo()
			req.setTTL(1)
			finErr := internal.Error("test error for operation finalizer")

			s := &coreOperationExecutor{
				pre: &testExecutionEntity{
					f: func(items ...interface{}) {
						t.Run("correct params computer arguments", func(t *testing.T) {
							require.Equal(t, computableParams{}, *items[0].(*computableParams))
							require.Equal(t, req, items[1].(transport.MetaInfo))
						})
					},
				},
				fin: &testExecutionEntity{
					f: func(items ...interface{}) {
						par := items[0].(operationParams)
						require.Equal(t, req, par.metaInfo)
						require.Equal(t, p, par.itemHandler)
					},
					err: finErr,
				},
				loc: new(testExecutionEntity),
			}

			require.EqualError(t,
				s.executeOperation(ctx, req, p),
				finErr.Error(),
			)
		})

		t.Run("zero ttl", func(t *testing.T) {
			p := new(testExecutionEntity)
			req := newRawPutInfo()
			finErr := internal.Error("test error for operation finalizer")

			s := &coreOperationExecutor{
				loc: &testExecutionEntity{
					f: func(items ...interface{}) {
						require.Equal(t, req, items[0])
						require.Equal(t, p, items[1])
					},
					err: finErr,
				},
			}

			require.EqualError(t,
				s.executeOperation(ctx, req, p),
				finErr.Error(),
			)
		})
	})
}

func Test_localStoreExecutor(t *testing.T) {
	ctx := context.TODO()
	addr := testObjectAddress(t)

	t.Run("put", func(t *testing.T) {
		epoch := uint64(100)
		obj := new(Object)
		putErr := internal.Error("test error for put")

		ls := &testExecutionEntity{
			f: func(items ...interface{}) {
				t.Run("correct local store put params", func(t *testing.T) {
					v, ok := items[0].(context.Context).Value(localstore.StoreEpochValue).(uint64)
					require.True(t, ok)
					require.Equal(t, epoch, v)

					require.Equal(t, obj, items[1].(*Object))
				})
			},
		}

		s := &localStoreExecutor{
			epochRecv: &testExecutionEntity{
				res: epoch,
			},
			localStore: ls,
		}

		require.NoError(t, s.putObject(ctx, obj))

		ls.err = putErr

		require.EqualError(t,
			s.putObject(ctx, obj),
			errPutLocal.Error(),
		)
	})

	t.Run("get", func(t *testing.T) {
		t.Run("error", func(t *testing.T) {
			getErr := internal.Error("test error for get")

			ls := &testExecutionEntity{
				f: func(items ...interface{}) {
					t.Run("correct local store get params", func(t *testing.T) {
						require.Equal(t, addr, items[0].(Address))
					})
				},
				err: getErr,
			}

			s := &localStoreExecutor{
				localStore: ls,
			}

			res, err := s.getObject(ctx, addr)
			require.EqualError(t, err, getErr.Error())
			require.Nil(t, res)

			ls.err = errors.Wrap(core.ErrNotFound, "wrap message")

			res, err = s.getObject(ctx, addr)
			require.EqualError(t, err, errIncompleteOperation.Error())
			require.Nil(t, res)
		})

		t.Run("success", func(t *testing.T) {
			obj := new(Object)

			s := &localStoreExecutor{
				localStore: &testExecutionEntity{
					res: obj,
				},
			}

			res, err := s.getObject(ctx, addr)
			require.NoError(t, err)
			require.Equal(t, obj, res)
		})
	})

	t.Run("head", func(t *testing.T) {
		t.Run("error", func(t *testing.T) {
			headErr := internal.Error("test error for head")

			ls := &testExecutionEntity{
				err: headErr,
			}

			s := &localStoreExecutor{
				localStore: ls,
			}

			res, err := s.headObject(ctx, addr)
			require.EqualError(t, err, headErr.Error())
			require.Nil(t, res)

			ls.err = errors.Wrap(core.ErrNotFound, "wrap message")

			res, err = s.headObject(ctx, addr)
			require.EqualError(t, err, errIncompleteOperation.Error())
			require.Nil(t, res)
		})

		t.Run("success", func(t *testing.T) {
			obj := new(Object)

			s := &localStoreExecutor{
				localStore: &testExecutionEntity{
					res: &Meta{Object: obj},
				},
			}

			res, err := s.headObject(ctx, addr)
			require.NoError(t, err)
			require.Equal(t, obj, res)
		})
	})

	t.Run("get range", func(t *testing.T) {
		t.Run("error", func(t *testing.T) {
			rngErr := internal.Error("test error for range reader")

			s := &localStoreExecutor{
				localStore: &testExecutionEntity{
					err: rngErr,
				},
			}

			res, err := s.getRange(ctx, addr, Range{})
			require.EqualError(t, err, rngErr.Error())
			require.Empty(t, res)
		})

		t.Run("success", func(t *testing.T) {
			rng := Range{Offset: 1, Length: 1}

			d := testData(t, 10)

			s := &localStoreExecutor{
				localStore: &testExecutionEntity{
					f: func(items ...interface{}) {
						t.Run("correct local store pread params", func(t *testing.T) {
							require.Equal(t, addr, items[0].(Address))
							require.Equal(t, rng, items[1].(Range))
						})
					},
					res: d,
				},
			}

			res, err := s.getRange(ctx, addr, rng)
			require.NoError(t, err)
			require.Equal(t, d, res)
		})
	})

	t.Run("get range hash", func(t *testing.T) {
		t.Run("empty range list", func(t *testing.T) {
			s := &localStoreExecutor{
				localStore: new(testExecutionEntity),
			}

			res, err := s.getHashes(ctx, addr, nil, nil)
			require.NoError(t, err)
			require.Empty(t, res)
		})

		t.Run("error", func(t *testing.T) {
			rhErr := internal.Error("test error for range hasher")

			s := &localStoreExecutor{
				localStore: &testExecutionEntity{
					err: rhErr,
				},
			}

			res, err := s.getHashes(ctx, addr, make([]Range, 1), nil)
			require.EqualError(t, err, errors.Wrapf(rhErr, emRangeReadFail, 1).Error())
			require.Empty(t, res)
		})

		t.Run("success", func(t *testing.T) {
			rngs := []Range{
				{Offset: 0, Length: 0},
				{Offset: 1, Length: 1},
			}

			d := testData(t, 64)
			salt := testData(t, 20)

			callNum := 0

			s := &localStoreExecutor{
				salitor: hash.SaltXOR,
				localStore: &testExecutionEntity{
					f: func(items ...interface{}) {
						t.Run("correct local store pread params", func(t *testing.T) {
							require.Equal(t, addr, items[0].(Address))
							require.Equal(t, rngs[callNum], items[1].(Range))
							callNum++
						})
					},
					res: d,
				},
			}

			res, err := s.getHashes(ctx, addr, rngs, salt)
			require.NoError(t, err)
			require.Len(t, res, len(rngs))
			for i := range rngs {
				require.Equal(t, hash.Sum(hash.SaltXOR(d, salt)), res[i])
			}
		})
	})
}

func Test_coreHandler_HandleResult(t *testing.T) {
	ctx := context.TODO()
	node := testNode(t, 1)

	t.Run("error", func(t *testing.T) {
		handled := false
		err := internal.Error("")

		s := &coreHandler{
			traverser: &testExecutionEntity{
				f: func(items ...interface{}) {
					t.Run("correct traverser params", func(t *testing.T) {
						require.Equal(t, node, items[0].(multiaddr.Multiaddr))
						require.False(t, items[1].(bool))
					})
				},
			},
			itemHandler: &testExecutionEntity{
				f: func(items ...interface{}) {
					handled = true
				},
			},
			resLogger: new(coreResultLogger),
		}

		s.HandleResult(ctx, node, nil, err)

		require.False(t, handled)
	})

	t.Run("success", func(t *testing.T) {
		handled := false
		res := testData(t, 10)

		s := &coreHandler{
			traverser: &testExecutionEntity{
				f: func(items ...interface{}) {
					t.Run("correct traverser params", func(t *testing.T) {
						require.Equal(t, node, items[0].(multiaddr.Multiaddr))
						require.True(t, items[1].(bool))
					})
				},
			},
			itemHandler: &testExecutionEntity{
				f: func(items ...interface{}) {
					require.Equal(t, res, items[0])
				},
			},
			resLogger: new(coreResultLogger),
		}

		s.HandleResult(ctx, node, res, nil)

		require.False(t, handled)
	})
}

func Test_localOperationExecutor_executeOperation(t *testing.T) {
	ctx := context.TODO()

	addr := testObjectAddress(t)

	obj := &Object{
		SystemHeader: SystemHeader{
			ID:  addr.ObjectID,
			CID: addr.CID,
		},
	}

	t.Run("wrong type", func(t *testing.T) {
		req := &testExecutionEntity{
			res: object.RequestType(-1),
		}

		require.EqualError(t,
			new(localOperationExecutor).executeOperation(ctx, req, nil),
			errors.Errorf(pmWrongRequestType, req).Error(),
		)
	})

	t.Run("put", func(t *testing.T) {
		req := &putRequest{PutRequest: &object.PutRequest{
			R: &object.PutRequest_Header{
				Header: &object.PutRequest_PutHeader{
					Object: obj,
				},
			},
		}}

		t.Run("error", func(t *testing.T) {
			putErr := internal.Error("test error for put")

			s := &localOperationExecutor{
				objStore: &testExecutionEntity{
					f: func(items ...interface{}) {
						require.Equal(t, obj, items[0].(*Object))
					},
					err: putErr,
				},
			}

			require.EqualError(t,
				s.executeOperation(ctx, req, nil),
				putErr.Error(),
			)
		})

		t.Run("success", func(t *testing.T) {
			h := &testExecutionEntity{
				f: func(items ...interface{}) {
					require.Equal(t, addr, *items[0].(*Address))
				},
			}

			s := &localOperationExecutor{
				objStore: new(testExecutionEntity),
			}

			require.NoError(t, s.executeOperation(ctx, req, h))
		})
	})

	t.Run("get", func(t *testing.T) {
		req := newRawGetInfo()
		req.setAddress(addr)

		t.Run("error", func(t *testing.T) {
			getErr := internal.Error("test error for get")

			s := &localOperationExecutor{
				objRecv: &testExecutionEntity{
					f: func(items ...interface{}) {
						require.Equal(t, addr, items[0].(Address))
					},
					err: getErr,
				},
			}

			require.EqualError(t,
				s.executeOperation(ctx, req, nil),
				getErr.Error(),
			)
		})

		t.Run("success", func(t *testing.T) {
			h := &testExecutionEntity{
				f: func(items ...interface{}) {
					require.Equal(t, obj, items[0].(*Object))
				},
			}

			s := &localOperationExecutor{
				objRecv: &testExecutionEntity{
					res: obj,
				},
			}

			require.NoError(t, s.executeOperation(ctx, req, h))
		})
	})

	t.Run("head", func(t *testing.T) {
		req := &transportRequest{serviceRequest: &object.HeadRequest{
			Address: addr,
		}}

		t.Run("error", func(t *testing.T) {
			headErr := internal.Error("test error for head")

			s := &localOperationExecutor{
				headRecv: &testExecutionEntity{
					f: func(items ...interface{}) {
						require.Equal(t, addr, items[0].(Address))
					},
					err: headErr,
				},
			}

			require.EqualError(t,
				s.executeOperation(ctx, req, nil),
				headErr.Error(),
			)
		})

		t.Run("success", func(t *testing.T) {
			h := &testExecutionEntity{
				f: func(items ...interface{}) {
					require.Equal(t, obj, items[0].(*Object))
				},
			}

			s := &localOperationExecutor{
				headRecv: &testExecutionEntity{
					res: obj,
				},
			}

			require.NoError(t, s.executeOperation(ctx, req, h))
		})
	})

	t.Run("search", func(t *testing.T) {
		cid := testObjectAddress(t).CID
		testQuery := testData(t, 10)

		req := &transportRequest{serviceRequest: &object.SearchRequest{
			ContainerID: cid,
			Query:       testQuery,
		}}

		t.Run("error", func(t *testing.T) {
			searchErr := internal.Error("test error for search")

			s := &localOperationExecutor{
				queryImp: &testExecutionEntity{
					f: func(items ...interface{}) {
						require.Equal(t, cid, items[0].(CID))
						require.Equal(t, testQuery, items[1].([]byte))
						require.Equal(t, 1, items[2].(int))
					},
					err: searchErr,
				},
			}

			require.EqualError(t,
				s.executeOperation(ctx, req, nil),
				searchErr.Error(),
			)
		})

		t.Run("success", func(t *testing.T) {
			addrList := testAddrList(t, 5)

			h := &testExecutionEntity{
				f: func(items ...interface{}) {
					require.Equal(t, addrList, items[0].([]Address))
				},
			}

			s := &localOperationExecutor{
				queryImp: &testExecutionEntity{
					res: addrList,
				},
			}

			require.NoError(t, s.executeOperation(ctx, req, h))
		})
	})

	t.Run("get range", func(t *testing.T) {
		rng := Range{Offset: 1, Length: 1}

		req := newRawRangeInfo()
		req.setAddress(addr)
		req.setRange(rng)

		t.Run("error", func(t *testing.T) {
			rrErr := internal.Error("test error for range reader")

			s := &localOperationExecutor{
				rngReader: &testExecutionEntity{
					f: func(items ...interface{}) {
						require.Equal(t, addr, items[0].(Address))
						require.Equal(t, rng, items[1].(Range))
					},
					err: rrErr,
				},
			}

			require.EqualError(t,
				s.executeOperation(ctx, req, nil),
				rrErr.Error(),
			)
		})

		t.Run("success", func(t *testing.T) {
			data := testData(t, 10)

			h := &testExecutionEntity{
				f: func(items ...interface{}) {
					d, err := ioutil.ReadAll(items[0].(io.Reader))
					require.NoError(t, err)
					require.Equal(t, data, d)
				},
			}

			s := &localOperationExecutor{
				rngReader: &testExecutionEntity{
					res: data,
				},
			}

			require.NoError(t, s.executeOperation(ctx, req, h))
		})
	})

	t.Run("get range hash", func(t *testing.T) {
		rngs := []Range{
			{Offset: 0, Length: 0},
			{Offset: 1, Length: 1},
		}

		salt := testData(t, 10)

		req := newRawRangeHashInfo()
		req.setAddress(addr)
		req.setRanges(rngs)
		req.setSalt(salt)

		t.Run("error", func(t *testing.T) {
			rhErr := internal.Error("test error for range hasher")

			s := &localOperationExecutor{
				rngHasher: &testExecutionEntity{
					f: func(items ...interface{}) {
						require.Equal(t, addr, items[0].(Address))
						require.Equal(t, rngs, items[1].([]Range))
						require.Equal(t, salt, items[2].([]byte))
					},
					err: rhErr,
				},
			}

			require.EqualError(t,
				s.executeOperation(ctx, req, nil),
				rhErr.Error(),
			)
		})

		t.Run("success", func(t *testing.T) {
			hashes := []Hash{
				hash.Sum(testData(t, 10)),
				hash.Sum(testData(t, 10)),
			}

			h := &testExecutionEntity{
				f: func(items ...interface{}) {
					require.Equal(t, hashes, items[0].([]Hash))
				},
			}

			s := &localOperationExecutor{
				rngHasher: &testExecutionEntity{
					res: hashes,
				},
			}

			require.NoError(t, s.executeOperation(ctx, req, h))
		})
	})
}

func Test_coreOperationFinalizer_completeExecution(t *testing.T) {
	ctx := context.TODO()

	t.Run("address store failure", func(t *testing.T) {
		asErr := internal.Error("test error for address store")

		s := &coreOperationFinalizer{
			interceptorPreparer: &testExecutionEntity{
				err: asErr,
			},
		}

		require.EqualError(t, s.completeExecution(ctx, operationParams{
			metaInfo: &transportRequest{serviceRequest: new(object.SearchRequest)},
		}), asErr.Error())
	})

	t.Run("correct execution construction", func(t *testing.T) {
		req := &transportRequest{
			serviceRequest: &object.SearchRequest{
				ContainerID:  testObjectAddress(t).CID,
				Query:        testData(t, 10),
				QueryVersion: 1,
			},
			timeout: 10 * time.Second,
		}

		req.SetTTL(10)

		itemHandler := new(testExecutionEntity)
		opParams := operationParams{
			computableParams: computableParams{
				addr:               testObjectAddress(t),
				stopCount:          2,
				allowPartialResult: false,
				tryPreviousNetMap:  false,
				selfForward:        true,
				maxRecycleCount:    7,
			},
			metaInfo:    req,
			itemHandler: itemHandler,
		}

		curPl := new(testExecutionEntity)
		prevPl := new(testExecutionEntity)
		wp := new(testExecutionEntity)
		s := &coreOperationFinalizer{
			curPlacementBuilder:  curPl,
			prevPlacementBuilder: prevPl,
			interceptorPreparer: &testExecutionEntity{
				res: func(context.Context, multiaddr.Multiaddr) bool { return true },
			},
			workerPool: wp,
			traverseExec: &testExecutionEntity{
				f: func(items ...interface{}) {
					t.Run("correct traverse executor params", func(t *testing.T) {
						p := items[0].(implementations.TraverseParams)

						require.True(t, p.ExecutionInterceptor(ctx, nil))
						require.Equal(t, req, p.TransportInfo)
						require.Equal(t, wp, p.WorkerPool)

						tr := p.Traverser.(*coreTraverser)
						require.Equal(t, opParams.addr, tr.addr)
						require.Equal(t, opParams.tryPreviousNetMap, tr.tryPrevNM)
						require.Equal(t, curPl, tr.curPlacementBuilder)
						require.Equal(t, prevPl, tr.prevPlacementBuilder)
						require.Equal(t, opParams.maxRecycleCount, tr.maxRecycleCount)
						require.Equal(t, opParams.stopCount, tr.stopCount)

						h := p.Handler.(*coreHandler)
						require.Equal(t, tr, h.traverser)
						require.Equal(t, itemHandler, h.itemHandler)
					})
				},
			},
			log: zap.L(),
		}

		require.EqualError(t, s.completeExecution(ctx, opParams), errIncompleteOperation.Error())
	})
}

func Test_coreInterceptorPreparer_prepareInterceptor(t *testing.T) {
	t.Run("address store failure", func(t *testing.T) {
		asErr := internal.Error("test error for address store")

		s := &coreInterceptorPreparer{
			addressStore: &testExecutionEntity{
				err: asErr,
			},
		}

		res, err := s.prepareInterceptor(interceptorItems{})
		require.EqualError(t, err, asErr.Error())
		require.Nil(t, res)
	})

	t.Run("correct interceptor", func(t *testing.T) {
		ctx := context.TODO()
		selfAddr := testNode(t, 0)

		t.Run("local node", func(t *testing.T) {
			req := new(transportRequest)
			itemHandler := new(testExecutionEntity)

			localErr := internal.Error("test error for local executor")

			p := interceptorItems{
				selfForward: true,
				handler: &testExecutionEntity{
					f: func(items ...interface{}) {
						t.Run("correct local executor params", func(t *testing.T) {
							require.Equal(t, selfAddr, items[0].(multiaddr.Multiaddr))
							require.Nil(t, items[1])
							require.EqualError(t, items[2].(error), localErr.Error())
						})
					},
				},
				metaInfo:    req,
				itemHandler: itemHandler,
			}

			s := &coreInterceptorPreparer{
				localExec: &testExecutionEntity{
					f: func(items ...interface{}) {
						require.Equal(t, req, items[0].(transport.MetaInfo))
						require.Equal(t, itemHandler, items[1].(responseItemHandler))
					},
					err: localErr,
				},
				addressStore: &testExecutionEntity{
					res: selfAddr,
				},
			}

			res, err := s.prepareInterceptor(p)
			require.NoError(t, err)
			require.False(t, res(ctx, selfAddr))
		})

		t.Run("remote node", func(t *testing.T) {
			node := testNode(t, 1)
			remoteNode := testNode(t, 2)

			p := interceptorItems{}

			s := &coreInterceptorPreparer{
				addressStore: &testExecutionEntity{
					res: remoteNode,
				},
			}

			res, err := s.prepareInterceptor(p)
			require.NoError(t, err)
			require.False(t, res(ctx, node))
		})
	})
}

// testAddrList returns count random object addresses.
func testAddrList(t *testing.T, count int) (res []Address) {
	for i := 0; i < count; i++ {
		res = append(res, testObjectAddress(t))
	}
	return
}
