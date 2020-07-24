package object

import (
	"context"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-api-go/query"
	v1 "github.com/nspcc-dev/neofs-api-go/query"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testListingEntity struct {
		// Set of interfaces which entity must implement, but some methods from those does not call.
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
	_ objectSearcher         = (*testListingEntity)(nil)
	_ selectiveRangeReceiver = (*testListingEntity)(nil)

	_ transport.SelectiveContainerExecutor = (*testListingEntity)(nil)
)

func (s *testListingEntity) rangeDescriptor(_ context.Context, a Address, f relationQueryFunc) (RangeDescriptor, error) {
	if s.f != nil {
		s.f(a, f)
	}
	if s.err != nil {
		return RangeDescriptor{}, s.err
	}
	return s.res.(RangeDescriptor), nil
}

func (s *testListingEntity) Head(_ context.Context, p *transport.HeadParams) error {
	if s.f != nil {
		s.f(p)
	}
	return s.err
}

func (s *testListingEntity) searchObjects(ctx context.Context, i transport.SearchInfo) ([]Address, error) {
	if s.f != nil {
		s.f(i)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]Address), nil
}

func Test_rawSeachInfo(t *testing.T) {
	t.Run("TTL", func(t *testing.T) {
		ttl := uint32(3)

		r := newRawSearchInfo()
		r.setTTL(ttl)

		require.Equal(t, ttl, r.GetTTL())
	})

	t.Run("timeout", func(t *testing.T) {
		timeout := 3 * time.Second

		r := newRawSearchInfo()
		r.setTimeout(timeout)

		require.Equal(t, timeout, r.GetTimeout())
	})

	t.Run("CID", func(t *testing.T) {
		cid := testObjectAddress(t).CID

		r := newRawSearchInfo()
		r.setCID(cid)

		require.Equal(t, cid, r.GetCID())
	})

	t.Run("query", func(t *testing.T) {
		query := testData(t, 10)

		r := newRawSearchInfo()
		r.setQuery(query)

		require.Equal(t, query, r.GetQuery())
	})
}

func Test_coreChildrenQueryFunc(t *testing.T) {
	t.Run("correct query composition", func(t *testing.T) {
		// create custom address for test
		addr := testObjectAddress(t)

		res, err := coreChildrenQueryFunc(addr)
		require.NoError(t, err)

		// unmarshal query
		q := v1.Query{}
		require.NoError(t, q.Unmarshal(res))

		// ascertain that filter list composed correctly
		require.Len(t, q.Filters, 2)

		require.Contains(t, q.Filters, QueryFilter{
			Type: v1.Filter_Exact,
			Name: transport.KeyHasParent,
		})

		require.Contains(t, q.Filters, QueryFilter{
			Type:  v1.Filter_Exact,
			Name:  transport.KeyParent,
			Value: addr.ObjectID.String(),
		})
	})
}

func Test_coreChildrenLister_children(t *testing.T) {
	ctx := context.TODO()
	addr := testObjectAddress(t)

	t.Run("query function failure", func(t *testing.T) {
		s := &coreChildrenLister{
			queryFn: func(v Address) ([]byte, error) {
				t.Run("correct query function params", func(t *testing.T) {
					require.Equal(t, addr, v)
				})
				return nil, errors.New("") // force relationQueryFunc to return some non-nil error
			},
			log: test.NewLogger(false),
		}

		require.Empty(t, s.children(ctx, addr))
	})

	t.Run("object searcher failure", func(t *testing.T) {
		// create custom timeout for test
		sErr := errors.New("test error for object searcher")
		// create custom timeout for test
		timeout := 3 * time.Second
		// create custom query for test
		query := testData(t, 10)

		s := &coreChildrenLister{
			queryFn: func(v Address) ([]byte, error) {
				return query, nil // force relationQueryFunc to return created query
			},
			objSearcher: &testListingEntity{
				f: func(items ...interface{}) {
					t.Run("correct object searcher params", func(t *testing.T) {
						p := items[0].(transport.SearchInfo)
						require.Equal(t, timeout, p.GetTimeout())
						require.Equal(t, query, p.GetQuery())
						require.Equal(t, addr.CID, p.GetCID())
						require.Equal(t, uint32(service.NonForwardingTTL), p.GetTTL())
					})
				},
				err: sErr, // force objectSearcher to return sErr
			},
			log:     test.NewLogger(false),
			timeout: timeout,
		}

		require.Empty(t, s.children(ctx, addr))
	})

	t.Run("correct result", func(t *testing.T) {
		// create custom child list
		addrList := testAddrList(t, 5)
		idList := make([]ID, 0, len(addrList))
		for i := range addrList {
			idList = append(idList, addrList[i].ObjectID)
		}

		s := &coreChildrenLister{
			queryFn: func(address Address) ([]byte, error) {
				return nil, nil // force relationQueryFunc to return nil error
			},
			objSearcher: &testListingEntity{
				res: addrList,
			},
		}

		require.Equal(t, idList, s.children(ctx, addr))
	})
}

func Test_queryGenerators(t *testing.T) {
	t.Run("object ID", func(t *testing.T) {
		var (
			q   = new(query.Query)
			key = "key for test"
			id  = testObjectAddress(t).ObjectID
		)

		res, err := idQueryFunc(key, id)
		require.NoError(t, err)

		require.NoError(t, q.Unmarshal(res))
		require.Len(t, q.Filters, 1)

		require.Equal(t, query.Filter{
			Type:  v1.Filter_Exact,
			Name:  key,
			Value: id.String(),
		}, q.Filters[0])
	})

	t.Run("left neighbor", func(t *testing.T) {
		var (
			q    = new(query.Query)
			addr = testObjectAddress(t)
		)

		res, err := leftNeighborQueryFunc(addr)
		require.NoError(t, err)

		require.NoError(t, q.Unmarshal(res))
		require.Len(t, q.Filters, 1)

		require.Equal(t, query.Filter{
			Type:  v1.Filter_Exact,
			Name:  KeyNext,
			Value: addr.ObjectID.String(),
		}, q.Filters[0])
	})

	t.Run("right neighbor", func(t *testing.T) {
		var (
			q    = new(query.Query)
			addr = testObjectAddress(t)
		)

		res, err := rightNeighborQueryFunc(addr)
		require.NoError(t, err)

		require.NoError(t, q.Unmarshal(res))
		require.Len(t, q.Filters, 1)

		require.Equal(t, query.Filter{
			Type:  v1.Filter_Exact,
			Name:  KeyPrev,
			Value: addr.ObjectID.String(),
		}, q.Filters[0])
	})

	t.Run("first child", func(t *testing.T) {
		var (
			q    = new(query.Query)
			addr = testObjectAddress(t)
		)

		res, err := firstChildQueryFunc(addr)
		require.NoError(t, err)

		require.NoError(t, q.Unmarshal(res))
		require.Len(t, q.Filters, 3)

		require.Contains(t, q.Filters, query.Filter{
			Type: v1.Filter_Exact,
			Name: transport.KeyHasParent,
		})
		require.Contains(t, q.Filters, query.Filter{
			Type:  v1.Filter_Exact,
			Name:  transport.KeyParent,
			Value: addr.ObjectID.String(),
		})
		require.Contains(t, q.Filters, query.Filter{
			Type:  v1.Filter_Exact,
			Name:  KeyPrev,
			Value: ID{}.String(),
		})
	})
}

func Test_selectiveRangeRecv(t *testing.T) {
	ctx := context.TODO()
	addr := testObjectAddress(t)

	t.Run("query function failure", func(t *testing.T) {
		qfErr := errors.New("test error for query function")
		_, err := new(selectiveRangeRecv).rangeDescriptor(ctx, testObjectAddress(t), func(Address) ([]byte, error) {
			return nil, qfErr
		})
		require.EqualError(t, err, qfErr.Error())
	})

	t.Run("correct executor params", func(t *testing.T) {
		t.Run("w/ query function", func(t *testing.T) {
			qBytes := testData(t, 10)

			s := &selectiveRangeRecv{
				executor: &testListingEntity{
					f: func(items ...interface{}) {
						p := items[0].(*transport.HeadParams)
						require.Equal(t, addr.CID, p.CID)
						require.True(t, p.ServeLocal)
						require.Equal(t, uint32(service.SingleForwardingTTL), p.TTL)
						require.True(t, p.FullHeaders)
						require.Equal(t, qBytes, p.Query)
						require.Empty(t, p.IDList)
					},
				},
			}

			_, _ = s.rangeDescriptor(ctx, addr, func(Address) ([]byte, error) { return qBytes, nil })
		})

		t.Run("w/o query function", func(t *testing.T) {
			s := &selectiveRangeRecv{
				executor: &testListingEntity{
					f: func(items ...interface{}) {
						p := items[0].(*transport.HeadParams)
						require.Equal(t, addr.CID, p.CID)
						require.True(t, p.ServeLocal)
						require.Equal(t, uint32(service.SingleForwardingTTL), p.TTL)
						require.True(t, p.FullHeaders)
						require.Empty(t, p.Query)
						require.Equal(t, []ID{addr.ObjectID}, p.IDList)
					},
				},
			}

			_, _ = s.rangeDescriptor(ctx, addr, nil)
		})
	})

	t.Run("correct result", func(t *testing.T) {
		t.Run("failure", func(t *testing.T) {
			t.Run("executor failure", func(t *testing.T) {
				exErr := errors.New("test error for executor")

				s := &selectiveRangeRecv{
					executor: &testListingEntity{
						err: exErr,
					},
				}

				_, err := s.rangeDescriptor(ctx, addr, nil)
				require.EqualError(t, err, exErr.Error())
			})

			t.Run("not found", func(t *testing.T) {
				s := &selectiveRangeRecv{
					executor: new(testListingEntity),
				}

				_, err := s.rangeDescriptor(ctx, addr, nil)
				require.EqualError(t, err, errRelationNotFound.Error())
			})
		})

		t.Run("success", func(t *testing.T) {
			foundAddr := testObjectAddress(t)

			obj := &Object{
				SystemHeader: SystemHeader{
					PayloadLength: 100,
					ID:            foundAddr.ObjectID,
					CID:           foundAddr.CID,
				},
			}

			s := &selectiveRangeRecv{
				executor: &testListingEntity{
					SelectiveContainerExecutor: nil,
					f: func(items ...interface{}) {
						p := items[0].(*transport.HeadParams)
						p.Handler(nil, obj)
					},
				},
			}

			res, err := s.rangeDescriptor(ctx, addr, nil)
			require.NoError(t, err)
			require.Equal(t, RangeDescriptor{
				Size:   int64(obj.SystemHeader.PayloadLength),
				Offset: 0,
				Addr:   foundAddr,

				LeftBound:  true,
				RightBound: true,
			}, res)
		})
	})
}

func Test_neighborReceiver(t *testing.T) {
	ctx := context.TODO()
	addr := testObjectAddress(t)

	t.Run("neighbor", func(t *testing.T) {
		t.Run("correct internal logic", func(t *testing.T) {
			rightCalled, leftCalled := false, false

			s := &neighborReceiver{
				leftNeighborQueryFn: func(a Address) ([]byte, error) {
					require.Equal(t, addr, a)
					leftCalled = true
					return nil, nil
				},
				rightNeighborQueryFn: func(a Address) ([]byte, error) {
					require.Equal(t, addr, a)
					rightCalled = true
					return nil, nil
				},
				rangeDescRecv: &testListingEntity{
					f: func(items ...interface{}) {
						require.Equal(t, addr, items[0])
						_, _ = items[1].(relationQueryFunc)(addr)
					},
					err: errors.New(""),
				},
			}

			_, _ = s.Neighbor(ctx, addr, true)
			require.False(t, rightCalled)
			require.True(t, leftCalled)

			leftCalled = false

			_, _ = s.Neighbor(ctx, addr, false)
			require.False(t, leftCalled)
			require.True(t, rightCalled)
		})

		t.Run("correct result", func(t *testing.T) {
			rErr := errors.New("test error for range receiver")

			rngRecv := &testListingEntity{err: rErr}
			s := &neighborReceiver{rangeDescRecv: rngRecv}

			_, err := s.Neighbor(ctx, addr, false)
			require.EqualError(t, err, rErr.Error())

			rngRecv.err = errRelationNotFound

			_, err = s.Neighbor(ctx, addr, false)
			require.EqualError(t, err, errRelationNotFound.Error())

			rd := RangeDescriptor{Size: 1, Offset: 2, Addr: addr}
			rngRecv.res, rngRecv.err = rd, nil

			res, err := s.Neighbor(ctx, addr, false)
			require.NoError(t, err)
			require.Equal(t, rd, res)
		})
	})

	t.Run("base", func(t *testing.T) {
		rd := RangeDescriptor{Size: 1, Offset: 2, Addr: addr}

		t.Run("first child exists", func(t *testing.T) {
			called := false

			s := &neighborReceiver{
				firstChildQueryFn: func(a Address) ([]byte, error) {
					require.Equal(t, addr, a)
					called = true
					return nil, nil
				},
				rangeDescRecv: &testListingEntity{
					f: func(items ...interface{}) {
						require.Equal(t, addr, items[0])
						_, _ = items[1].(relationQueryFunc)(addr)
					},
					res: rd,
				},
			}

			res, err := s.Base(ctx, addr)
			require.NoError(t, err)
			require.Equal(t, rd, res)
			require.True(t, called)
		})

		t.Run("first child doesn't exist", func(t *testing.T) {
			called := false

			recv := &testListingEntity{err: errors.New("")}

			recv.f = func(...interface{}) {
				if called {
					recv.res, recv.err = rd, nil
				}
				called = true
			}

			s := &neighborReceiver{rangeDescRecv: recv}

			res, err := s.Base(ctx, addr)
			require.NoError(t, err)
			require.Equal(t, rd, res)
		})
	})
}
