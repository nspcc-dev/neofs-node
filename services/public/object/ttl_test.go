package object

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testTTLEntity struct {
		// Set of interfaces which testCommonEntity must implement, but some methods from those does not call.
		serviceRequest
		Placer

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var (
	_ ttlConditionPreparer         = (*testTTLEntity)(nil)
	_ implementations.AddressStore = (*testTTLEntity)(nil)
	_ containerAffiliationChecker  = (*testTTLEntity)(nil)
	_ Placer                       = (*testTTLEntity)(nil)
)

func (s *testTTLEntity) SelfAddr() (multiaddr.Multiaddr, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(multiaddr.Multiaddr), nil
}

func (s *testTTLEntity) IsContainerNode(_ context.Context, m multiaddr.Multiaddr, c CID, b bool) (bool, error) {
	if s.f != nil {
		s.f(m, c, b)
	}
	if s.err != nil {
		return false, s.err
	}
	return s.res.(bool), nil
}

func (s *testTTLEntity) CID() CID { return s.res.([]interface{})[0].(CID) }

func (s *testTTLEntity) AllowPreviousNetMap() bool { return s.res.([]interface{})[1].(bool) }

func (s *testTTLEntity) prepareTTLCondition(_ context.Context, req object.Request) service.TTLCondition {
	if s.f != nil {
		s.f(req)
	}
	return s.res.(service.TTLCondition)
}

func (s *testTTLEntity) affiliated(ctx context.Context, cid CID) containerAffiliationResult {
	if s.f != nil {
		s.f(cid)
	}
	return s.res.(containerAffiliationResult)
}

func Test_ttlPreProcessor_preProcess(t *testing.T) {
	ctx := context.TODO()

	// create custom request with forwarding TTL
	req := &testTTLEntity{res: uint32(service.SingleForwardingTTL)}

	t.Run("empty request", func(t *testing.T) {
		require.PanicsWithValue(t, pmEmptyServiceRequest, func() {
			// ascertain that nil request causes panic
			_ = new(ttlPreProcessor).preProcess(ctx, nil)
		})
	})

	t.Run("correct processing", func(t *testing.T) {
		// create custom error
		pErr := internal.Error("test error for processing func")

		// create custom ttlConditionPreparer
		condPreparer := &testTTLEntity{
			f: func(items ...interface{}) {
				t.Run("correct condition preparer params", func(t *testing.T) {
					// ascertain that request argument of ttlPreProcessor and ttlConditionPreparer are the same
					require.Equal(t, req, items[0].(object.Request))
				})
			},
			res: service.TTLCondition(func(uint32) error { return nil }),
		}

		s := &ttlPreProcessor{
			condPreps: []ttlConditionPreparer{condPreparer},
			fProc: func(service.TTLSource, ...service.TTLCondition) error {
				return pErr // force processing function to return created error
			},
		}

		// ascertain error returns as expected
		require.EqualError(t,
			s.preProcess(ctx, req),
			pErr.Error(),
		)
	})
}

func Test_coreTTLCondPreparer_prepareTTLCondition(t *testing.T) {
	ctx := context.TODO()

	// create container ID
	cid := testObjectAddress(t).CID

	// // create network address
	// mAddr := testNode(t, 0)
	//
	// // create custom AddressStore
	// as := &testTTLEntity{
	// 	res: mAddr, // force AddressStore to return created address
	// }

	t.Run("empty request", func(t *testing.T) {
		require.PanicsWithValue(t, pmEmptyServiceRequest, func() {
			// ascertain that nil request causes panic
			_ = new(coreTTLCondPreparer).prepareTTLCondition(ctx, nil)
		})
	})

	t.Run("forwarding TTL", func(t *testing.T) {
		s := &coreTTLCondPreparer{
			curAffChecker:  new(testTTLEntity),
			prevAffChecker: new(testTTLEntity),
		}

		cond := s.prepareTTLCondition(ctx, new(testTTLEntity))

		// ascertain that error returns as expected
		require.NoError(t, cond(service.SingleForwardingTTL))
	})

	t.Run("non-forwarding TTL", func(t *testing.T) {
		t.Run("container non-affiliation", func(t *testing.T) {
			t.Run("disallow previous epoch affiliation", func(t *testing.T) {
				// create custom serviceRequest for test
				req := &testTTLEntity{res: []interface{}{
					cid,   // force serviceRequest to return cid
					false, // force serviceRequest to disallow previous network map
				}}

				s := &coreTTLCondPreparer{
					curAffChecker: &testTTLEntity{
						f: func(items ...interface{}) {
							t.Run("correct current epoch affiliation checker params", func(t *testing.T) {
								require.Equal(t, cid, items[0].(CID))
							})
						},
						res: affAbsence, // force current epoch containerAffiliationChecker to return affAbsence
					},
					prevAffChecker: &testTTLEntity{
						f: func(items ...interface{}) {
							t.Run("correct previous epoch affiliation checker params", func(t *testing.T) {
								require.Equal(t, cid, items[0].(CID))
							})
						},
						res: affPresence, // force previous epoch containerAffiliationChecker to return affPresence
					},
				}

				cond := s.prepareTTLCondition(ctx, req)

				// ascertain that error returns as expected
				require.EqualError(t,
					cond(service.SingleForwardingTTL-1), // pass any non-forwarding TTL
					errNotLocalContainer.Error(),
				)
			})

			t.Run("allow previous epoch affiliation", func(t *testing.T) {
				// create custom serviceRequest for test
				req := &testTTLEntity{res: []interface{}{
					cid,  // force serviceRequest to return cid
					true, // force serviceRequest to allow previous network map
				}}

				s := &coreTTLCondPreparer{
					curAffChecker: &testTTLEntity{
						res: affAbsence, // force current epoch containerAffiliationChecker to return affAbsence
					},
					prevAffChecker: &testTTLEntity{
						res: affAbsence, // force previous epoch containerAffiliationChecker to return affAbsence
					},
				}

				cond := s.prepareTTLCondition(ctx, req)

				// ascertain that error returns as expected
				require.EqualError(t,
					cond(service.SingleForwardingTTL-1), // pass any non-forwarding TTL
					errNotLocalContainer.Error(),
				)
			})
		})

		t.Run("container affiliation", func(t *testing.T) {
			t.Run("disallow previous epoch affiliation", func(t *testing.T) {
				// create custom serviceRequest for test
				req := &testTTLEntity{res: []interface{}{
					cid,   // force serviceRequest to return cid
					false, // force serviceRequest to disallow previous network map
				}}

				s := &coreTTLCondPreparer{
					curAffChecker: &testTTLEntity{
						res: affPresence, // force current epoch containerAffiliationChecker to return affPresence
					},
					prevAffChecker: &testTTLEntity{
						res: affAbsence, // force previous epoch containerAffiliationChecker to return affAbsence
					},
				}

				cond := s.prepareTTLCondition(ctx, req)

				// ascertain that error returns as expected
				require.NoError(t,
					cond(service.SingleForwardingTTL-1), // pass any non-forwarding TTL
				)
			})

			t.Run("allow previous epoch affiliation", func(t *testing.T) {
				// create custom serviceRequest for test
				req := &testTTLEntity{res: []interface{}{
					cid,  // force serviceRequest to return cid
					true, // force serviceRequest to allow previous network map
				}}

				s := &coreTTLCondPreparer{
					curAffChecker: &testTTLEntity{
						res: affAbsence, // force current epoch containerAffiliationChecker to return affAbsence
					},
					prevAffChecker: &testTTLEntity{
						res: affPresence, // force previous epoch containerAffiliationChecker to return affPresence
					},
				}

				cond := s.prepareTTLCondition(ctx, req)

				// ascertain that error returns as expected
				require.NoError(t,
					cond(service.SingleForwardingTTL-1), // pass any non-forwarding TTL
				)
			})
		})
	})
}

func Test_coreCnrAffChecker_affiliated(t *testing.T) {
	ctx := context.TODO()

	// create container ID
	cid := testObjectAddress(t).CID

	log := zap.L()

	t.Run("local network address store error", func(t *testing.T) {
		// create custom error for test
		saErr := internal.Error("test error for self addr store")

		s := &corePlacementUtil{
			localAddrStore: &testTTLEntity{
				err: saErr, // force address store to return saErr
			},
			log: log,
		}

		require.Equal(t, affUnknown, s.affiliated(ctx, cid))
	})

	t.Run("placement build result", func(t *testing.T) {
		// create network address
		mAddr := testNode(t, 0)

		// create custom AddressStore
		as := &testTTLEntity{
			res: mAddr, // force AddressStore to return created address
		}

		t.Run("error", func(t *testing.T) {
			pb := &testTTLEntity{
				f: func(items ...interface{}) {
					t.Run("correct placement builder params", func(t *testing.T) {
						require.Equal(t, mAddr, items[0].(multiaddr.Multiaddr))
						require.Equal(t, cid, items[1].(CID))
						require.Equal(t, true, items[2].(bool))
					})
				},
			}

			pb.err = internal.Error("") // force Placer to return some non-nil error

			s := &corePlacementUtil{
				prevNetMap:       true,
				localAddrStore:   as,
				placementBuilder: pb,
				log:              log,
			}

			require.Equal(t, affUnknown, s.affiliated(ctx, cid))

			pb.err = container.ErrNotFound

			require.Equal(t, affNotFound, s.affiliated(ctx, cid))
		})

		t.Run("no error", func(t *testing.T) {
			t.Run("affiliation", func(t *testing.T) {
				s := &corePlacementUtil{
					localAddrStore: as,
					placementBuilder: &testTTLEntity{
						res: true, // force Placer to return true, nil
					},
					log: log,
				}

				require.Equal(t, affPresence, s.affiliated(ctx, cid))
			})

			t.Run("non-affiliation", func(t *testing.T) {
				s := &corePlacementUtil{
					localAddrStore: as,
					placementBuilder: &testTTLEntity{
						res: false, // force Placer to return false, nil
					},
					log: log,
				}

				require.Equal(t, affAbsence, s.affiliated(ctx, cid))
			})
		})
	})
}

// testNode returns 0.0.0.0:(8000+num).
func testNode(t *testing.T, num int) multiaddr.Multiaddr {
	mAddr, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/" + strconv.Itoa(8000+num))
	require.NoError(t, err)
	return mAddr
}

// testObjectAddress returns new random object address.
func testObjectAddress(t *testing.T) Address {
	oid, err := refs.NewObjectID()
	require.NoError(t, err)
	return Address{CID: refs.CIDForBytes(testData(t, refs.CIDSize)), ObjectID: oid}
}

// testData returns size bytes of random data.
func testData(t *testing.T, size int) []byte {
	res := make([]byte, size)
	_, err := rand.Read(res)
	require.NoError(t, err)
	return res
}
