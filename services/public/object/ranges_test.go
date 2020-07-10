package object

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/objio"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testRangeEntity struct {
		// Set of interfaces which entity must implement, but some methods from those does not call.
		RangeChopper
		object.Service_GetRangeServer

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var (
	_ objio.RelativeReceiver        = (*testRangeEntity)(nil)
	_ RangeChopper                  = (*testRangeEntity)(nil)
	_ operationExecutor             = (*testRangeEntity)(nil)
	_ requestHandler                = (*testRangeEntity)(nil)
	_ rangeRevealer                 = (*testRangeEntity)(nil)
	_ objectRangeReceiver           = (*testRangeEntity)(nil)
	_ object.Service_GetRangeServer = (*testRangeEntity)(nil)
	_ responsePreparer              = (*testRangeEntity)(nil)
)

func (s *testRangeEntity) prepareResponse(_ context.Context, req serviceRequest, resp serviceResponse) error {
	if s.f != nil {
		s.f(req, resp)
	}
	return s.err
}

func (s *testRangeEntity) Context() context.Context { return context.TODO() }

func (s *testRangeEntity) Send(r *GetRangeResponse) error {
	if s.f != nil {
		s.f(r)
	}
	return s.err
}

func (s *testRangeEntity) getRange(_ context.Context, t rangeTool) (interface{}, error) {
	if s.f != nil {
		s.f(t)
	}
	return s.res, s.err
}

func (s *testRangeEntity) reveal(_ context.Context, r *RangeDescriptor) ([]RangeDescriptor, error) {
	if s.f != nil {
		s.f(r)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]RangeDescriptor), nil
}

func (s *testRangeEntity) Base(ctx context.Context, addr Address) (RangeDescriptor, error) {
	if s.f != nil {
		s.f(addr)
	}
	if s.err != nil {
		return RangeDescriptor{}, s.err
	}
	return s.res.(RangeDescriptor), nil
}

func (s *testRangeEntity) Neighbor(ctx context.Context, addr Address, left bool) (RangeDescriptor, error) {
	if s.f != nil {
		s.f(addr, left)
	}
	if s.err != nil {
		return RangeDescriptor{}, s.err
	}
	return s.res.(RangeDescriptor), nil
}

func (s *testRangeEntity) Chop(ctx context.Context, length, offset int64, fromStart bool) ([]RangeDescriptor, error) {
	if s.f != nil {
		s.f(length, offset, fromStart)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]RangeDescriptor), nil
}

func (s *testRangeEntity) Closed() bool { return s.res.(bool) }

func (s *testRangeEntity) PutChopper(addr Address, chopper RangeChopper) error {
	if s.f != nil {
		s.f(addr, chopper)
	}
	return s.err
}

func (s *testRangeEntity) GetChopper(addr Address, rc objio.RCType) (RangeChopper, error) {
	if s.f != nil {
		s.f(addr, rc)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(RangeChopper), nil
}

func (s *testRangeEntity) executeOperation(_ context.Context, i transport.MetaInfo, h responseItemHandler) error {
	if s.f != nil {
		s.f(i, h)
	}
	return s.err
}

func (s *testRangeEntity) handleRequest(_ context.Context, p handleRequestParams) (interface{}, error) {
	if s.f != nil {
		s.f(p)
	}
	return s.res, s.err
}

func Test_objectService_GetRange(t *testing.T) {
	req := &GetRangeRequest{Address: testObjectAddress(t)}

	t.Run("request handler error", func(t *testing.T) {
		rhErr := internal.Error("test error for request handler")

		s := &objectService{
			statusCalculator: newStatusCalculator(),
		}

		s.requestHandler = &testRangeEntity{
			f: func(items ...interface{}) {
				t.Run("correct request handler params", func(t *testing.T) {
					p := items[0].(handleRequestParams)
					require.Equal(t, s, p.executor)
					require.Equal(t, req, p.request)
				})
			},
			err: rhErr, // force requestHandler to return rhErr
		}

		// ascertain that error returns as expected
		require.EqualError(t, s.GetRange(req, new(testRangeEntity)), rhErr.Error())
	})

	t.Run("correct result", func(t *testing.T) {
		fragment := testData(t, 10)

		resp := &GetRangeResponse{Fragment: fragment}

		s := objectService{
			requestHandler: &testRangeEntity{
				res: bytes.NewReader(fragment), // force requestHandler to return fragment
			},
			rangeChunkPreparer: &testRangeEntity{
				f: func(items ...interface{}) {
					require.Equal(t, req, items[0])
					require.Equal(t, makeRangeResponse(fragment), items[1])
				},
				res: resp,
			},

			statusCalculator: newStatusCalculator(),
		}

		srv := &testRangeEntity{
			f: func(items ...interface{}) {
				require.Equal(t, resp, items[0])
			},
		}

		require.NoError(t, s.GetRange(req, srv))
	})
}

func Test_objectService_GetRangeHash(t *testing.T) {
	ctx := context.TODO()

	req := &GetRangeHashRequest{Address: testObjectAddress(t)}

	t.Run("request handler error", func(t *testing.T) {
		rhErr := internal.Error("test error for request handler")

		s := &objectService{
			statusCalculator: newStatusCalculator(),
		}

		s.requestHandler = &testRangeEntity{
			f: func(items ...interface{}) {
				t.Run("correct request handler params", func(t *testing.T) {
					p := items[0].(handleRequestParams)
					require.Equal(t, s, p.executor)
					require.Equal(t, req, p.request)
				})
			},
			err: rhErr, // force requestHandler to return rhErr
		}

		// ascertain that error returns as expected
		res, err := s.GetRangeHash(ctx, req)
		require.EqualError(t, err, rhErr.Error())
		require.Nil(t, res)
	})

	t.Run("correct result", func(t *testing.T) {
		hCount := 5
		hashes := make([]Hash, 0, hCount)

		for i := 0; i < hCount; i++ {
			hashes = append(hashes, hash.Sum(testData(t, 10)))
		}

		s := objectService{
			requestHandler: &testRangeEntity{
				res: hashes, // force requestHandler to return fragments
			},
			respPreparer: &testRangeEntity{
				f: func(items ...interface{}) {
					require.Equal(t, req, items[0])
					require.Equal(t, makeRangeHashResponse(hashes), items[1])
				},
				res: &GetRangeHashResponse{Hashes: hashes},
			},

			statusCalculator: newStatusCalculator(),
		}

		res, err := s.GetRangeHash(ctx, req)
		require.NoError(t, err)
		require.Equal(t, hashes, res.Hashes)
	})
}

func Test_coreRangeReceiver(t *testing.T) {
	ctx := context.TODO()
	log := zap.L()

	t.Run("range reveal failure", func(t *testing.T) {
		revErr := internal.Error("test error for range revealer")

		rt := newRawRangeHashInfo()
		rt.setTTL(service.NonForwardingTTL)
		rt.setAddress(testObjectAddress(t))
		rt.setRanges([]Range{
			{
				Offset: 1,
				Length: 2,
			},
		})

		revealer := &testRangeEntity{
			f: func(items ...interface{}) {
				require.Equal(t, &RangeDescriptor{
					Size:   int64(rt.rngList[0].Length),
					Offset: int64(rt.rngList[0].Offset),
					Addr:   rt.addr,
				}, items[0])
			},
			err: revErr,
		}

		s := &coreRangeReceiver{
			rngRevealer: revealer,
			log:         log,
		}

		res, err := s.getRange(ctx, rt)
		require.EqualError(t, err, errPayloadRangeNotFound.Error())
		require.Nil(t, res)

		revealer.err = nil
		revealer.res = make([]RangeDescriptor, 0)

		res, err = s.getRange(ctx, rt)
		require.EqualError(t, err, errPayloadRangeNotFound.Error())
		require.Nil(t, res)
	})

	t.Run("get sub range failure", func(t *testing.T) {
		gErr := internal.Error("test error for get range")

		rt := newRawRangeHashInfo()
		rt.setTTL(service.NonForwardingTTL)
		rt.setAddress(testObjectAddress(t))
		rt.setRanges([]Range{
			{
				Offset: 1,
				Length: 2,
			},
		})

		revealer := &testRangeEntity{
			res: []RangeDescriptor{{Size: 3, Offset: 4, Addr: testObjectAddress(t)}},
		}

		called := false
		revealer.f = func(items ...interface{}) {
			if called {
				revealer.err = gErr
				return
			}
			called = true
		}

		s := &coreRangeReceiver{
			rngRevealer: revealer,
			log:         log,
		}

		res, err := s.getRange(ctx, rt)
		require.EqualError(t, err, errPayloadRangeNotFound.Error())
		require.Nil(t, res)
	})

	t.Run("non-forwarding behavior", func(t *testing.T) {
		rt := newRawRangeHashInfo()
		rt.setTTL(service.NonForwardingTTL - 1)
		rt.setAddress(testObjectAddress(t))
		rt.setRanges([]Range{
			{
				Offset: 1,
				Length: 2,
			},
		})

		rd := RangeDescriptor{
			Size:   int64(rt.rngList[0].Length),
			Offset: int64(rt.rngList[0].Offset),
			Addr:   rt.addr,
		}

		d := hash.Sum(testData(t, 10))

		s := &coreRangeReceiver{
			straightRngRecv: &testRangeEntity{
				f: func(items ...interface{}) {
					require.Equal(t, rt.budOff(&rd), items[0])
				},
				res: d,
			},
		}

		res, err := s.getRange(ctx, rt)
		require.NoError(t, err)
		require.Equal(t, d, res)
	})

	t.Run("correct result concat", func(t *testing.T) {
		rt := newRawRangeHashInfo()
		rt.setTTL(service.NonForwardingTTL)
		rt.setRanges([]Range{
			{},
		})

		revealer := new(testRangeEntity)
		revCalled := false
		revealer.f = func(items ...interface{}) {
			if revCalled {
				revealer.res = []RangeDescriptor{items[0].(RangeDescriptor)}
			} else {
				revealer.res = make([]RangeDescriptor, 2)
			}
			revCalled = true
		}

		h1, h2 := hash.Sum(testData(t, 10)), hash.Sum(testData(t, 10))

		recvCalled := false
		receiver := new(testRangeEntity)
		receiver.f = func(...interface{}) {
			if recvCalled {
				receiver.res = h2
			} else {
				receiver.res = h1
			}
			recvCalled = true
		}

		s := &coreRangeReceiver{
			rngRevealer:     revealer,
			straightRngRecv: receiver,
		}

		exp, err := hash.Concat([]Hash{h1, h2})
		require.NoError(t, err)

		res, err := s.getRange(ctx, rt)
		require.NoError(t, err)
		require.Equal(t, exp, res)
	})
}

func Test_straightRangeReceiver_getRange(t *testing.T) {
	ctx := context.TODO()

	req := new(transportRequest)

	t.Run("executor error", func(t *testing.T) {
		exErr := internal.Error("test error for executor")

		s := &straightRangeReceiver{
			executor: &testRangeEntity{
				f: func(items ...interface{}) {
					t.Run("correct executor params", func(t *testing.T) {
						require.Equal(t, req, items[0])
						require.Equal(t, newSingleItemHandler(), items[1])
					})
				},
				err: exErr, // force operationExecutor to return exErr
			},
		}

		res, err := s.getRange(ctx, req)
		require.EqualError(t, err, exErr.Error())
		require.Nil(t, res)
	})

	t.Run("correct result", func(t *testing.T) {
		v := testData(t, 10)

		s := &straightRangeReceiver{
			executor: &testRangeEntity{
				f: func(items ...interface{}) {
					items[1].(rangeItemAccumulator).handleItem(v)
				},
				err: nil, // force operationExecutor to return nil error
			},
		}

		res, err := s.getRange(ctx, req)
		require.NoError(t, err)
		require.Equal(t, v, res)
	})
}

func Test_coreRngRevealer_reveal(t *testing.T) {
	ctx := context.TODO()

	rd := RangeDescriptor{
		Size:   5,
		Offset: 6,
		Addr:   testObjectAddress(t),
	}

	t.Run("charybdis chopper presence", func(t *testing.T) {
		cErr := internal.Error("test error for charybdis")

		s := &coreRngRevealer{
			chopTable: &testRangeEntity{
				f: func(items ...interface{}) {
					t.Run("correct chopper table params", func(t *testing.T) {
						require.Equal(t, rd.Addr, items[0])
						require.Equal(t, objio.RCCharybdis, items[1])
					})
				},
				res: &testRangeEntity{
					f: func(items ...interface{}) {
						t.Run("correct chopper params", func(t *testing.T) {
							require.Equal(t, rd.Size, items[0])
							require.Equal(t, rd.Offset, items[1])
							require.True(t, items[2].(bool))
						})
					},
					res: true, // close chopper
					err: cErr, // force RangeChopper to return cErr
				},
			},
		}

		res, err := s.reveal(ctx, &rd)
		require.EqualError(t, err, cErr.Error())
		require.Empty(t, res)
	})

	t.Run("scylla chopper presence", func(t *testing.T) {
		scErr := internal.Error("test error for scylla")

		scylla := &testRangeEntity{
			err: scErr, // force RangeChopper to return scErr
		}

		ct := new(testRangeEntity)

		ct.f = func(items ...interface{}) {
			if items[1].(objio.RCType) == objio.RCCharybdis {
				ct.err = internal.Error("")
			} else {
				ct.res = scylla
				ct.err = nil
			}
		}

		s := &coreRngRevealer{
			chopTable: ct,
		}

		res, err := s.reveal(ctx, &rd)
		require.EqualError(t, err, scErr.Error())
		require.Empty(t, res)
	})

	t.Run("new scylla", func(t *testing.T) {
		t.Run("error", func(t *testing.T) {
			s := &coreRngRevealer{
				relativeRecv: nil, // pass empty relation receiver to fail constructor
				chopTable: &testRangeEntity{
					err: internal.Error(""), // force ChopperTable to return non-nil error
				},
			}

			res, err := s.reveal(ctx, &rd)
			require.Error(t, err)
			require.Nil(t, res)
		})

		t.Run("success", func(t *testing.T) {
			rrErr := internal.Error("test error for relative receiver")

			relRecv := &testRangeEntity{
				err: rrErr, // force relative receiver to return rrErr
			}

			scylla, err := objio.NewScylla(&objio.ChopperParams{
				RelativeReceiver: relRecv,
				Addr:             rd.Addr,
			})
			require.NoError(t, err)

			callNum := 0

			s := &coreRngRevealer{
				relativeRecv: relRecv,
				chopTable: &testRangeEntity{
					f: func(items ...interface{}) {
						t.Run("correct put chopper params", func(t *testing.T) {
							if callNum >= 2 {
								require.Equal(t, rd.Addr, items[0])
								require.Equal(t, scylla, items[1])
							}
						})
					},
					err: internal.Error(""), // force ChopperTable to return non-nil error
				},
			}

			expRes, expErr := scylla.Chop(ctx, rd.Size, rd.Offset, true)
			require.Error(t, expErr)

			res, err := s.reveal(ctx, &rd)
			require.EqualError(t, err, expErr.Error())
			require.Equal(t, expRes, res)
		})
	})
}

func Test_transportRequest_rangeTool(t *testing.T) {
	t.Run("get ranges", func(t *testing.T) {
		rngs := []Range{
			{Offset: 1, Length: 2},
			{Offset: 3, Length: 4},
		}

		reqs := []transportRequest{
			{serviceRequest: &GetRangeHashRequest{Ranges: rngs}},
		}

		for i := range reqs {
			require.Equal(t, reqs[i].GetRanges(), rngs)
		}
	})

	t.Run("bud off", func(t *testing.T) {
		var (
			timeout = 6 * time.Second
			ttl     = uint32(16)
			rd      = RangeDescriptor{
				Size:   1,
				Offset: 2,
				Addr:   testObjectAddress(t),
			}
		)

		t.Run("get range hash request", func(t *testing.T) {
			salt := testData(t, 10)

			r := &GetRangeHashRequest{Salt: salt}
			r.SetToken(new(service.Token))

			req := &transportRequest{
				serviceRequest: r,
				timeout:        timeout,
			}
			req.SetTTL(ttl)

			tool := req.budOff(&rd).(transport.RangeHashInfo)

			require.Equal(t, timeout, tool.GetTimeout())
			require.Equal(t, ttl, tool.GetTTL())
			require.Equal(t, rd.Addr, tool.GetAddress())
			require.Equal(t, []Range{{Offset: uint64(rd.Offset), Length: uint64(rd.Size)}}, tool.GetRanges())
			require.Equal(t, salt, tool.GetSalt())
			require.Equal(t, r.GetSessionToken(), tool.GetSessionToken())
		})
	})

	t.Run("handler", func(t *testing.T) {
		t.Run("get range request", func(t *testing.T) {
			req := &transportRequest{serviceRequest: new(GetRangeHashRequest)}
			handler := req.handler()
			require.Equal(t, new(rangeHashAccum), handler)
		})
	})
}

func Test_rawRangeHashInfo(t *testing.T) {
	t.Run("get ranges", func(t *testing.T) {
		rngs := []Range{
			{Offset: 1, Length: 2},
			{Offset: 3, Length: 4},
		}

		r := newRawRangeHashInfo()
		r.setRanges(rngs)

		require.Equal(t, rngs, r.GetRanges())
	})

	t.Run("handler", func(t *testing.T) {
		require.Equal(t,
			&rangeHashAccum{concat: true},
			newRawRangeHashInfo().handler(),
		)
	})

	t.Run("bud off", func(t *testing.T) {
		var (
			ttl     = uint32(12)
			timeout = 7 * time.Hour
		)

		r := newRawRangeHashInfo()
		r.setTTL(ttl)
		r.setTimeout(timeout)
		r.setSalt(testData(t, 20))
		r.setSessionToken(new(service.Token))

		rd := RangeDescriptor{
			Size:   120,
			Offset: 71,
			Addr:   testObjectAddress(t),
		}

		tool := r.budOff(&rd)

		require.Equal(t, ttl, tool.GetTTL())
		require.Equal(t, timeout, tool.GetTimeout())
		require.Equal(t, rd.Addr, tool.GetAddress())
		require.Equal(t, []Range{{Offset: uint64(rd.Offset), Length: uint64(rd.Size)}}, tool.GetRanges())
		require.Equal(t, r.GetSessionToken(), tool.GetSessionToken())
		require.Equal(t,
			loopData(r.salt, int64(len(r.salt)), rd.Offset),
			tool.(transport.RangeHashInfo).GetSalt(),
		)
	})
}

func Test_rawRangeInfo(t *testing.T) {
	t.Run("get ranges", func(t *testing.T) {
		rng := Range{Offset: 1, Length: 2}

		r := newRawRangeInfo()
		r.setRange(rng)

		require.Equal(t, rng, r.GetRange())
	})
}

func Test_loopSalt(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		require.Empty(t, loopData(nil, 20, 10))
		require.Empty(t, loopData(make([]byte, 0), 20, 10))
	})

	t.Run("data part", func(t *testing.T) {
		var (
			off, size int64 = 10, 20
			d               = testData(t, 40)
		)
		require.Equal(t, d[off:off+size], loopData(d, size, off))
	})

	t.Run("with recycle", func(t *testing.T) {
		var (
			d    = testData(t, 40)
			off  = int64(len(d) / 2)
			size = 2 * off
		)

		require.Equal(t,
			append(d[off:], d[:size-off]...),
			loopData(d, size, off),
		)
	})
}

func Test_rangeHashAccum(t *testing.T) {
	t.Run("handle item", func(t *testing.T) {
		s := &rangeHashAccum{
			h: []Hash{hash.Sum(testData(t, 10))},
		}

		h := hash.Sum(testData(t, 10))

		exp := append(s.h, h)

		s.handleItem(h)

		require.Equal(t, exp, s.h)

		exp = append(s.h, s.h...)

		s.handleItem(s.h)

		require.Equal(t, exp, s.h)
	})

	t.Run("collect", func(t *testing.T) {
		hashes := []Hash{hash.Sum(testData(t, 10)), hash.Sum(testData(t, 10))}

		t.Run("w/ concat", func(t *testing.T) {
			s := &rangeHashAccum{
				concat: true,
				h:      hashes,
			}

			expRes, expErr := hash.Concat(hashes)

			res, err := s.collect()

			require.Equal(t, expRes, res)
			require.Equal(t, expErr, err)
		})

		t.Run("w/o concat", func(t *testing.T) {
			s := &rangeHashAccum{
				concat: false,
				h:      hashes,
			}

			res, err := s.collect()
			require.NoError(t, err)
			require.Equal(t, hashes, res)
		})
	})
}
