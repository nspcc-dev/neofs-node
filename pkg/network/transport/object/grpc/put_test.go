package object

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testPutEntity struct {
		// Set of interfaces which entity must implement, but some methods from those does not call.
		object.Service_PutServer
		transport.PutInfo
		Filter
		session.PrivateTokenStore
		transport.SelectiveContainerExecutor

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var (
	_ object.Service_PutServer  = (*testPutEntity)(nil)
	_ requestHandler            = (*testPutEntity)(nil)
	_ objectStorer              = (*testPutEntity)(nil)
	_ transport.PutInfo         = (*testPutEntity)(nil)
	_ Filter                    = (*testPutEntity)(nil)
	_ operationExecutor         = (*testPutEntity)(nil)
	_ session.PrivateTokenStore = (*testPutEntity)(nil)
	_ EpochReceiver             = (*testPutEntity)(nil)
	_ transformer.Transformer   = (*testPutEntity)(nil)
)

func (s *testPutEntity) Verify(_ context.Context, obj *Object) error {
	if s.f != nil {
		s.f(obj)
	}
	return s.err
}

func (s *testPutEntity) Transform(_ context.Context, u transformer.ProcUnit, h ...transformer.ProcUnitHandler) error {
	if s.f != nil {
		s.f(u, h)
	}
	return s.err
}

func (s *testPutEntity) verify(_ context.Context, token *session.Token, obj *Object) error {
	if s.f != nil {
		s.f(token, obj)
	}
	return s.err
}

func (s *testPutEntity) Epoch() uint64 { return s.res.(uint64) }

func (s *testPutEntity) Direct(ctx context.Context, objs ...Object) ([]Object, error) {
	if s.f != nil {
		s.f(ctx, objs)
	}
	return s.res.([]Object), s.err
}

func (s *testPutEntity) Fetch(id session.PrivateTokenKey) (session.PrivateToken, error) {
	if s.f != nil {
		s.f(id)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(session.PrivateToken), nil
}

func (s *testPutEntity) executeOperation(_ context.Context, m transport.MetaInfo, h responseItemHandler) error {
	if s.f != nil {
		s.f(m, h)
	}
	return s.err
}

func (s *testPutEntity) Pass(ctx context.Context, m *Meta) *localstore.FilterResult {
	if s.f != nil {
		s.f(ctx, m)
	}
	items := s.res.([]interface{})
	return items[0].(*localstore.FilterResult)
}

func (s *testPutEntity) GetTTL() uint32 { return s.res.(uint32) }

func (s *testPutEntity) GetToken() *session.Token { return s.res.(*session.Token) }

func (s *testPutEntity) GetHead() *Object { return s.res.(*Object) }

func (s *testPutEntity) putObject(ctx context.Context, p transport.PutInfo) (*Address, error) {
	if s.f != nil {
		s.f(p, ctx)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*Address), nil
}

func (s *testPutEntity) handleRequest(_ context.Context, p handleRequestParams) (interface{}, error) {
	if s.f != nil {
		s.f(p)
	}
	return s.res, s.err
}

func (s *testPutEntity) Recv() (*object.PutRequest, error) {
	if s.f != nil {
		s.f()
	}
	if s.err != nil {
		return nil, s.err
	} else if s.res == nil {
		return nil, nil
	}
	return s.res.(*object.PutRequest), nil
}

func (s *testPutEntity) Context() context.Context { return context.TODO() }

func Test_objectService_Put(t *testing.T) {

	t.Run("stream error", func(t *testing.T) {
		// create custom error for test
		psErr := errors.New("test error for put stream server")

		s := &testPutEntity{
			err: psErr, // force server to return psErr
		}

		srv := &objectService{
			statusCalculator: newStatusCalculator(),
		}

		// ascertain that error returns as expected
		require.EqualError(t,
			srv.Put(s),
			psErr.Error(),
		)
	})

	t.Run("request handling", func(t *testing.T) {
		// create custom request for test
		req := &object.PutRequest{R: &object.PutRequest_Header{
			Header: &object.PutRequest_PutHeader{
				Object: new(Object),
			},
		}}

		// create custom error for test
		hErr := errors.New("test error for request handler")

		srv := &testPutEntity{
			res: req, // force server to return req
		}

		s := &objectService{
			statusCalculator: newStatusCalculator(),
		}

		s.requestHandler = &testPutEntity{
			f: func(items ...interface{}) {
				t.Run("correct request handler params", func(t *testing.T) {
					p := items[0].(handleRequestParams)
					require.Equal(t, s, p.executor)
					require.Equal(t, &putRequest{
						PutRequest: req,
						srv:        srv,
					}, p.request)
				})
			},
			err: hErr, // force requestHandler to return hErr
		}

		// ascertain that error returns as expected
		require.EqualError(t,
			s.Put(srv),
			hErr.Error(),
		)
	})
}

func Test_straightObjectStorer_putObject(t *testing.T) {
	ctx := context.TODO()

	t.Run("executor error", func(t *testing.T) {
		// create custom error for test
		exErr := errors.New("test error for operation executor")

		// create custom meta info for test
		info := new(testPutEntity)

		s := &straightObjectStorer{
			executor: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct operation executor params", func(t *testing.T) {
						require.Equal(t, info, items[0])
						acc := items[1].(*coreAddrAccum)
						require.NotNil(t, acc.Once)
					})
				},
				err: exErr,
			},
		}

		_, err := s.putObject(ctx, info)

		// ascertain that error returns as expected
		require.EqualError(t, err, exErr.Error())
	})

	t.Run("correct result", func(t *testing.T) {
		addr := testObjectAddress(t)

		s := &straightObjectStorer{
			executor: &testPutEntity{
				f: func(items ...interface{}) {
					// add address to accumulator
					items[1].(addressAccumulator).handleItem(&addr)
				},
			},
		}

		res, err := s.putObject(ctx, new(testPutEntity))
		require.NoError(t, err)

		// ascertain that result returns as expected
		require.Equal(t, &addr, res)
	})
}

func Test_recvPutHeaderMsg(t *testing.T) {
	t.Run("server error", func(t *testing.T) {
		// create custom error for test
		srvErr := errors.New("test error for put server")

		srv := &testPutEntity{
			err: srvErr, // force put server to return srvErr
		}

		res, err := recvPutHeaderMsg(srv)

		// ascertain that error returns as expected
		require.EqualError(t, err, srvErr.Error())
		require.Nil(t, res)
	})

	t.Run("empty message", func(t *testing.T) {
		srv := &testPutEntity{
			res: nil, // force put server to return nil, nil
		}

		res, err := recvPutHeaderMsg(srv)

		// ascertain that error returns as expected
		require.EqualError(t, err, errHeaderExpected.Error())
		require.Nil(t, res)
	})

	t.Run("empty put header in message", func(t *testing.T) {
		srv := &testPutEntity{
			res: new(object.PutRequest), // force put server to return message w/o put header
		}

		res, err := recvPutHeaderMsg(srv)

		// ascertain that error returns as expected
		require.EqualError(t, err, object.ErrHeaderExpected.Error())
		require.Nil(t, res)
	})

	t.Run("empty object in put header", func(t *testing.T) {
		srv := &testPutEntity{
			res: object.MakePutRequestHeader(nil), // force put server to return message w/ nil object
		}

		res, err := recvPutHeaderMsg(srv)

		// ascertain that error returns as expected
		require.EqualError(t, err, errObjectExpected.Error())
		require.Nil(t, res)
	})
}

func Test_putRequest(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		timeout := 3 * time.Second

		req := &putRequest{
			timeout: timeout,
		}

		// ascertain that timeout returns as expected
		require.Equal(t, timeout, req.GetTimeout())
	})

	t.Run("head", func(t *testing.T) {
		// create custom object for test
		obj := new(Object)

		req := &putRequest{
			PutRequest: object.MakePutRequestHeader(obj), // wrap object to test message
		}

		// ascertain that head returns as expected
		require.Equal(t, obj, req.GetHead())
	})

	t.Run("payload", func(t *testing.T) {
		req := &putRequest{
			srv: new(testPutEntity),
		}

		require.Equal(t, &putStreamReader{srv: req.srv}, req.Payload())
	})

	t.Run("copies number", func(t *testing.T) {
		cn := uint32(5)

		req := &putRequest{
			PutRequest: &object.PutRequest{
				R: &object.PutRequest_Header{
					Header: &object.PutRequest_PutHeader{
						CopiesNumber: cn,
					},
				},
			},
		}

		require.Equal(t, cn, req.CopiesNumber())
	})
}

func Test_coreAddrAccum(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		s := newAddressAccumulator()
		// ascertain that type is correct and Once entity initialize
		require.NotNil(t, s.(*coreAddrAccum).Once)
	})

	t.Run("address", func(t *testing.T) {
		addr := testObjectAddress(t)

		s := &coreAddrAccum{addr: &addr}

		// ascertain that address returns as expected
		require.Equal(t, &addr, s.address())
	})

	t.Run("handle", func(t *testing.T) {
		addr := testObjectAddress(t)

		s := newAddressAccumulator()

		s.handleItem(&addr)

		// ascertain that address saved
		require.Equal(t, &addr, s.address())

		// create another address for test
		addr2 := testObjectAddress(t)

		s.handleItem(&addr2)

		// ascertain that second address is ignored
		require.Equal(t, &addr, s.address())
	})
}

func Test_rawPutInfo(t *testing.T) {
	t.Run("TTL", func(t *testing.T) {
		ttl := uint32(3)

		s := newRawPutInfo()
		s.setTTL(ttl)

		require.Equal(t, ttl, s.GetTTL())
	})

	t.Run("head", func(t *testing.T) {
		obj := new(Object)

		s := newRawPutInfo()
		s.setHead(obj)

		require.Equal(t, obj, s.GetHead())
	})

	t.Run("payload", func(t *testing.T) {
		// ascertain that nil chunk returns as expected
		r := bytes.NewBuffer(nil)

		req := newRawPutInfo()
		req.setPayload(r)

		require.Equal(t, r, req.Payload())
	})

	t.Run("token", func(t *testing.T) {
		// ascertain that nil token returns as expected
		require.Nil(t, newRawPutInfo().GetSessionToken())
	})

	t.Run("copies number", func(t *testing.T) {
		cn := uint32(100)

		s := newRawPutInfo()
		s.setCopiesNumber(cn)

		require.Equal(t, cn, s.CopiesNumber())
	})
}

func Test_contextWithValues(t *testing.T) {
	k1, k2 := "key 1", "key2"
	v1, v2 := "value 1", "value 2"

	ctx := contextWithValues(context.TODO(), k1, v1, k2, v2)

	// ascertain that all values added
	require.Equal(t, v1, ctx.Value(k1))
	require.Equal(t, v2, ctx.Value(k2))
}

func Test_bifurcatingObjectStorer(t *testing.T) {
	ctx := context.TODO()

	// create custom error for test
	sErr := errors.New("test error for object storer")

	t.Run("w/ token", func(t *testing.T) {
		// create custom request w/ token
		sk := test.DecodeKey(0)

		owner, err := refs.NewOwnerID(&sk.PublicKey)
		require.NoError(t, err)

		token := new(service.Token)
		token.SetOwnerID(owner)

		req := &putRequest{
			PutRequest: object.MakePutRequestHeader(new(Object)),
		}
		req.SetToken(token)
		require.NoError(t, requestSignFunc(sk, req))

		s := &bifurcatingObjectStorer{
			tokenStorer: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct token storer params", func(t *testing.T) {
						require.Equal(t, req, items[0])
					})
				},
				err: sErr, // force token storer to return sErr
			},
		}

		_, err = s.putObject(ctx, req)
		require.EqualError(t, err, sErr.Error())
	})

	t.Run("w/o token", func(t *testing.T) {
		// create custom request w/o token
		req := newRawPutInfo()
		require.Nil(t, req.GetSessionToken())

		s := &bifurcatingObjectStorer{
			straightStorer: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct token storer params", func(t *testing.T) {
						require.Equal(t, req, items[0])
					})
				},
				err: sErr, // force token storer to return sErr
			},
		}

		_, err := s.putObject(ctx, req)
		require.EqualError(t, err, sErr.Error())
	})
}

func TestWithTokenFromOwner(t *testing.T) {
	// nil request
	require.False(t, withTokenFromOwner(nil))

	// create test request
	req := &putRequest{
		PutRequest: new(object.PutRequest),
	}

	// w/o session token
	require.Nil(t, req.GetSessionToken())
	require.False(t, withTokenFromOwner(req))

	// create test session token and add it to request
	token := new(service.Token)
	req.SetToken(token)

	// w/o signatures
	require.False(t, withTokenFromOwner(req))

	// create test public key
	pk := &test.DecodeKey(0).PublicKey

	// add key-signature pair
	req.AddSignKey(nil, pk)

	// wrong token owner
	require.False(t, withTokenFromOwner(req))

	// set correct token owner
	owner, err := refs.NewOwnerID(pk)
	require.NoError(t, err)

	token.SetOwnerID(owner)

	require.True(t, withTokenFromOwner(req))
}

func Test_tokenObjectStorer(t *testing.T) {
	ctx := context.TODO()

	token := new(service.Token)
	token.SetID(session.TokenID{1, 2, 3})
	token.SetSignature(testData(t, 10))

	// create custom request w/ token and object for test
	req := newRawPutInfo()
	req.setSessionToken(token)
	req.setHead(&Object{
		Payload: testData(t, 10),
	})

	t.Run("token store failure", func(t *testing.T) {
		s := &tokenObjectStorer{
			tokenStore: &testPutEntity{
				err: errors.New(""), // force token store to return a non-nil error
			},
		}

		_, err := s.putObject(ctx, req)
		require.EqualError(t, err, errTokenRetrieval.Error())
	})

	t.Run("correct result", func(t *testing.T) {
		addr := testObjectAddress(t)

		pToken, err := session.NewPrivateToken(0)
		require.NoError(t, err)

		s := &tokenObjectStorer{
			tokenStore: &testPutEntity{
				res: pToken,
			},
			objStorer: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct object storer params", func(t *testing.T) {
						require.Equal(t, req, items[0])
						ctx := items[1].(context.Context)
						require.Equal(t, pToken, ctx.Value(transformer.PrivateSessionToken))
						require.Equal(t, token, ctx.Value(transformer.PublicSessionToken))
					})
				},
				res: &addr,
			},
		}

		res, err := s.putObject(ctx, req)
		require.NoError(t, err)
		require.Equal(t, addr, *res)
	})
}

func Test_filteringObjectStorer(t *testing.T) {
	ctx := context.TODO()

	t.Run("filter failure", func(t *testing.T) {
		var (
			ttl = uint32(5)
			obj = &Object{Payload: testData(t, 10)}
		)

		req := newRawPutInfo()
		req.setHead(obj)
		req.setTTL(ttl)

		s := &filteringObjectStorer{
			filter: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct filter params", func(t *testing.T) {
						require.Equal(t, &Meta{Object: obj}, items[1])
						ctx := items[0].(context.Context)
						require.Equal(t, ttl, ctx.Value(ttlValue))
					})
				},
				res: []interface{}{localstore.ResultFail()},
			},
		}

		_, err := s.putObject(ctx, req)
		require.EqualError(t, err, errObjectFilter.Error())
	})

	t.Run("correct result", func(t *testing.T) {
		req := newRawPutInfo()
		req.setHead(&Object{
			Payload: testData(t, 10),
		})

		addr := testObjectAddress(t)

		s := &filteringObjectStorer{
			filter: &testPutEntity{
				res: []interface{}{localstore.ResultPass()},
			},
			objStorer: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct object storer params", func(t *testing.T) {
						require.Equal(t, req, items[0])
					})
				},
				res: &addr,
			},
		}

		res, err := s.putObject(ctx, req)
		require.NoError(t, err)
		require.Equal(t, &addr, res)
	})
}

func Test_receivingObjectStorer(t *testing.T) {
	ctx := context.TODO()

	t.Run("cut payload", func(t *testing.T) {
		payload := testData(t, 10)

		req := newRawPutInfo()
		req.setHead(&Object{
			SystemHeader: SystemHeader{
				PayloadLength: uint64(len(payload)) + 1,
			},
		})
		req.setPayload(bytes.NewBuffer(payload))

		_, err := new(receivingObjectStorer).putObject(ctx, req)
		require.EqualError(t, err, transformer.ErrPayloadEOF.Error())
	})

	t.Run("payload verification failure", func(t *testing.T) {
		vErr := errors.New("payload verification error for test")

		req := newRawPutInfo()
		req.setHead(&Object{
			Payload: testData(t, 10),
		})

		s := &receivingObjectStorer{
			vPayload: &testPutEntity{
				f: func(items ...interface{}) {
					require.Equal(t, req.obj, items[0])
				},
				err: vErr,
			},
		}

		_, err := s.putObject(ctx, req)

		require.EqualError(t, err, errPayloadChecksum.Error())
	})

	t.Run("correct result", func(t *testing.T) {
		var (
			cn      = uint32(10)
			ttl     = uint32(5)
			timeout = 3 * time.Second
			payload = testData(t, 10)
			addr    = testObjectAddress(t)
		)

		obj := &Object{
			SystemHeader: SystemHeader{
				PayloadLength: uint64(len(payload)),
				ID:            addr.ObjectID,
				CID:           addr.CID,
			},
		}

		req := newRawPutInfo()
		req.setHead(obj)
		req.setPayload(bytes.NewBuffer(payload))
		req.setTimeout(timeout)
		req.setTTL(ttl)
		req.setCopiesNumber(cn)
		req.setSessionToken(new(service.Token))

		s := &receivingObjectStorer{
			straightStorer: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct straight storer params", func(t *testing.T) {
						exp := newRawPutInfo()
						exp.setHead(obj)
						exp.setTimeout(timeout)
						exp.setTTL(ttl)
						exp.setCopiesNumber(cn)
						exp.setSessionToken(req.GetSessionToken())

						require.Equal(t, exp, items[0])
					})
				},
				res: &addr,
			},
			vPayload: new(testPutEntity),
		}

		res, err := s.putObject(ctx, req)
		require.NoError(t, err)
		require.Equal(t, &addr, res)
	})
}

func Test_transformingObjectStorer(t *testing.T) {
	ctx := context.TODO()

	t.Run("correct behavior", func(t *testing.T) {
		var (
			tErr = errors.New("test error for transformer")
			addr = testObjectAddress(t)
			obj  = &Object{
				SystemHeader: SystemHeader{
					ID:  addr.ObjectID,
					CID: addr.CID,
				},
				Payload: testData(t, 10),
			}
		)

		req := newRawPutInfo()
		req.setHead(obj)
		req.setPayload(bytes.NewBuffer(obj.Payload))
		req.setTimeout(3 * time.Second)
		req.setTTL(5)
		req.setCopiesNumber(100)
		req.setSessionToken(new(service.Token))

		tr := &testPutEntity{
			f: func(items ...interface{}) {
				t.Run("correct transformer params", func(t *testing.T) {
					require.Equal(t, transformer.ProcUnit{
						Head:    req.obj,
						Payload: req.r,
					}, items[0])
					fns := items[1].([]transformer.ProcUnitHandler)
					require.Len(t, fns, 1)
					_ = fns[0](ctx, transformer.ProcUnit{
						Head:    req.obj,
						Payload: req.r,
					})
				})
			},
		}

		s := &transformingObjectStorer{
			transformer: tr,
			objStorer: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct object storer params", func(t *testing.T) {
						exp := newRawPutInfo()
						exp.setHead(req.GetHead())
						exp.setPayload(req.Payload())
						exp.setTimeout(req.GetTimeout())
						exp.setTTL(req.GetTTL())
						exp.setCopiesNumber(req.CopiesNumber())
						exp.setSessionToken(req.GetSessionToken())

						require.Equal(t, exp, items[0])
					})
				},
				err: errors.New(""),
			},
			mErr: map[error]struct{}{
				tErr: {},
			},
		}

		res, err := s.putObject(ctx, req)
		require.NoError(t, err)
		require.Equal(t, &addr, res)

		tr.err = tErr

		_, err = s.putObject(ctx, req)
		require.EqualError(t, err, tErr.Error())

		tr.err = errors.New("some other error")

		_, err = s.putObject(ctx, req)
		require.EqualError(t, err, errTransformer.Error())

		e := &transformerHandlerErr{
			error: errors.New("transformer handler error"),
		}

		tr.err = e

		_, err = s.putObject(ctx, req)
		require.EqualError(t, err, e.error.Error())
	})
}

func Test_putStreamReader(t *testing.T) {
	t.Run("empty server", func(t *testing.T) {
		s := new(putStreamReader)
		n, err := s.Read(make([]byte, 1))
		require.EqualError(t, err, io.EOF.Error())
		require.Zero(t, n)
	})

	t.Run("fail presence", func(t *testing.T) {
		initTail := testData(t, 10)

		s := putStreamReader{
			tail: initTail,
			srv:  new(testPutEntity),
		}

		buf := make([]byte, len(s.tail)/2)

		n, err := s.Read(buf)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		require.Equal(t, buf, initTail[:n])
		require.Equal(t, initTail[n:], s.tail)
	})

	t.Run("receive message failure", func(t *testing.T) {
		t.Run("stream problem", func(t *testing.T) {
			srvErr := errors.New("test error for stream server")

			s := &putStreamReader{
				srv: &testPutEntity{
					err: srvErr,
				},
			}

			n, err := s.Read(make([]byte, 1))
			require.EqualError(t, err, srvErr.Error())
			require.Zero(t, n)
		})

		t.Run("incorrect chunk", func(t *testing.T) {
			t.Run("empty data", func(t *testing.T) {
				s := &putStreamReader{
					srv: &testPutEntity{
						res: object.MakePutRequestChunk(make([]byte, 0)),
					},
				}

				n, err := s.Read(make([]byte, 1))
				require.EqualError(t, err, errChunkExpected.Error())
				require.Zero(t, n)
			})

			t.Run("wrong message type", func(t *testing.T) {
				s := &putStreamReader{
					srv: &testPutEntity{
						res: object.MakePutRequestHeader(new(Object)),
					},
				}

				n, err := s.Read(make([]byte, 1))
				require.EqualError(t, err, errChunkExpected.Error())
				require.Zero(t, n)
			})
		})
	})

	t.Run("correct read", func(t *testing.T) {
		chunk := testData(t, 10)
		buf := make([]byte, len(chunk)/2)

		s := &putStreamReader{
			srv: &testPutEntity{
				res: object.MakePutRequestChunk(chunk),
			},
		}

		n, err := s.Read(buf)
		require.NoError(t, err)
		require.Equal(t, chunk[:n], buf)
		require.Equal(t, chunk[n:], s.tail)
	})

	t.Run("ful read", func(t *testing.T) {
		var (
			callNum        = 0
			chunk1, chunk2 = testData(t, 100), testData(t, 88)
		)

		srv := new(testPutEntity)
		srv.f = func(items ...interface{}) {
			if callNum == 0 {
				srv.res = object.MakePutRequestChunk(chunk1)
			} else if callNum == 1 {
				srv.res = object.MakePutRequestChunk(chunk2)
			} else {
				srv.res, srv.err = 0, io.EOF
			}
			callNum++
		}

		s := &putStreamReader{
			srv: srv,
		}

		var (
			n   int
			err error
			res = make([]byte, 0)
			buf = make([]byte, 10)
		)

		for err != io.EOF {
			n, err = s.Read(buf)
			res = append(res, buf[:n]...)
		}

		require.Equal(t, append(chunk1, chunk2...), res)
	})
}
