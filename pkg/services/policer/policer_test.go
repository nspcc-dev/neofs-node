package policer

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type storageListerWithDelay struct {
	ch chan struct{}

	m    sync.RWMutex
	objs []objectcore.AddressWithAttributes
	err  error
}

func (s *storageListerWithDelay) setListResulsts(oo []objectcore.AddressWithAttributes, err error) {
	s.m.Lock()
	s.objs = oo
	s.err = err
	s.m.Unlock()
}

func (s *storageListerWithDelay) ListWithCursor(u uint32, cursor *engine.Cursor, s2 ...string) ([]objectcore.AddressWithAttributes, *engine.Cursor, error) {
	<-s.ch
	s.m.RLock()
	defer s.m.RUnlock()
	return s.objs, cursor, s.err
}

func (s *storageListerWithDelay) Delete(address oid.Address) error {
	panic("do not call me")
}

func (s *storageListerWithDelay) DeleteRedundantCopies(address oid.Address, _ []string) error {
	panic("do not call me")
}

func (s *storageListerWithDelay) Put(o *object.Object, i []byte) error {
	panic("do not call me")
}

func (s *storageListerWithDelay) Head(address oid.Address, b bool) (*object.Object, error) {
	panic("do not call me")
}

func (s *storageListerWithDelay) HeadECPart(id cid.ID, id2 oid.ID, info iec.PartInfo) (object.Object, error) {
	panic("do not call me")
}

func (s *storageListerWithDelay) GetRange(address oid.Address, u uint64, u2 uint64) ([]byte, error) {
	panic("do not call me")
}

func TestConsistencyAndPlacement(t *testing.T) {
	t.Run("startup value", func(t *testing.T) {
		var (
			mockM     = &mockMetrics{}
			mockNet   = newMockNetwork()
			localNode = newTestLocalNode()
		)

		p := New(neofscryptotest.Signer(),
			WithReplicationCooldown(time.Hour),
			WithNetwork(mockNet),
			WithLogger(zap.NewNop()),
			WithMetrics(mockM),
		)
		p.localStorage = localNode

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		p.Run(ctx)

		require.Equal(t, false, mockM.consistency.Load())
		require.Equal(t, false, mockM.optimalPlacement.Load())
	})

	t.Run("metrics change", func(t *testing.T) {
		var (
			cnr      = cidtest.ID()
			objID    = oidtest.ID()
			objAddr  = oid.NewAddress(cnr, objID)
			localObj = objectcore.AddressWithAttributes{
				Address:    objAddr,
				Type:       object.TypeRegular,
				Attributes: make([]string, 3),
			}

			mockM     = &mockMetrics{}
			mockNet   = newMockNetwork()
			delayCh   = make(chan struct{})
			localNode = &storageListerWithDelay{ch: delayCh}
		)

		localNode.objs = []objectcore.AddressWithAttributes{localObj}

		p := New(neofscryptotest.Signer(),
			WithReplicationCooldown(time.Millisecond),
			WithNetwork(mockNet),
			WithLogger(zap.NewNop()),
			WithMetrics(mockM),
		)
		p.localStorage = localNode

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go p.Run(ctx)

		require.Equal(t, false, mockM.consistency.Load())
		require.Equal(t, false, mockM.optimalPlacement.Load())
		delayCh <- struct{}{}
		require.Equal(t, false, mockM.consistency.Load())      // still not finished cycle
		require.Equal(t, false, mockM.optimalPlacement.Load()) // still not finished cycle

		localNode.setListResulsts(nil, engine.ErrEndOfListing)
		delayCh <- struct{}{}
		delayCh <- struct{}{}
		require.Eventually(t, func() bool {
			return mockM.consistency.Load() && mockM.optimalPlacement.Load()
		}, 3*time.Second, 50*time.Millisecond)
		require.EqualValues(t, 1, mockM.processedRep.Load())
		require.EqualValues(t, 0, mockM.processedEC.Load())
		require.GreaterOrEqual(t, mockM.cycleCount.Load(), uint64(1))

		delayCh <- struct{}{}
	})

	t.Run("misplaced replica don't affect consistency", func(t *testing.T) {
		nodes := testutil.Nodes(3)
		cnr := cidtest.ID()
		objID := oidtest.ID()
		addr := oid.NewAddress(cnr, objID)

		mockM := &mockMetrics{}
		mockM.SetPolicerConsistency(true)
		mockM.SetPolicerOptimalPlacement(true)

		mockNet := newMockNetwork()
		mockNet.pubKey = nodes[0].PublicKey()
		mockNet.inNetmap = true

		conns := newMockAPIConnections()
		conns.setHeadResult(nodes[1], addr, apistatus.ErrObjectNotFound)
		conns.setHeadResult(nodes[2], addr, nil)

		r := newTestReplicator(t)
		r.success = true

		p := New(neofscryptotest.Signer(),
			WithNetwork(mockNet),
			WithLogger(zap.NewNop()),
			WithMetrics(mockM),
		)
		p.apiConns = conns
		p.replicator = r

		plc := &processPlacementContext{
			object: objectcore.AddressWithAttributes{
				Address: addr,
				Type:    object.TypeRegular,
			},
			checkedNodes: newNodeCache(mockM, false),
		}

		p.processNodes(context.Background(), plc, nodes, 2)

		require.True(t, mockM.consistency.Load())
		require.False(t, mockM.optimalPlacement.Load())
		require.True(t, p.hadPlacementMismatch.Load())
		require.False(t, p.hadReplicaShortage.Load())
		require.EqualValues(t, 1, mockM.replicatedRep.Load())
		require.EqualValues(t, 0, mockM.replicatedEC.Load())

		var exp replicator.Task
		exp.SetObjectAddress(addr)
		exp.SetCopiesNumber(1)
		exp.SetNodes([]netmap.NodeInfo{nodes[1]})
		require.Equal(t, exp, r.task.Load().(replicator.Task))
	})

	t.Run("deleted objects are split by mode", func(t *testing.T) {
		localNode := newTestLocalNode()
		mockM := &mockMetrics{}
		p := New(neofscryptotest.Signer(), WithMetrics(mockM), WithLogger(zap.NewNop()))
		p.localStorage = localNode

		repAddr := oid.NewAddress(cidtest.ID(), oidtest.ID())
		ecAddr := oid.NewAddress(cidtest.ID(), oidtest.ID())

		p.dropRedundantLocalObject(repAddr, false)
		p.dropRedundantLocalObject(ecAddr, true)

		require.EqualValues(t, 1, mockM.deletedRep.Load())
		require.EqualValues(t, 1, mockM.deletedEC.Load())
	})
}

func TestPolicer_Run_RepDefault(t *testing.T) {
	for _, typ := range []object.Type{
		object.TypeRegular,
		object.TypeTombstone,
		object.TypeLock,
		object.TypeLink,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			testDefaultREPWithType(t, typ)
		})
	}
}

func testDefaultREPWithType(t *testing.T, typ object.Type) {
	const defaultRep = 3
	const defaultCBF = 3
	nodes := testutil.Nodes(defaultRep * defaultCBF)
	cnr := cidtest.ID()
	objID := oidtest.ID()
	objAddr := oid.NewAddress(cnr, objID)
	broadcast := typ == object.TypeLock || typ == object.TypeLink

	localObj := objectcore.AddressWithAttributes{
		Address:    objAddr,
		Type:       typ,
		Attributes: make([]string, 3),
	}

	t.Run("all OK", func(t *testing.T) {
		allOK := islices.RepeatElement(len(nodes), error(nil))
		t.Run("in container", func(t *testing.T) {
			t.Run("primary", func(t *testing.T) {
				for i := range defaultRep {
					testRepCheck(t, defaultRep, localObj, nodes, i, true, allOK, 0, false, nil)
				}
			})
			t.Run("backup", func(t *testing.T) {
				for i := defaultRep; i < len(nodes); i++ {
					logBuf := testRepCheck(t, defaultRep, localObj, nodes, i, true, allOK, 0, !broadcast, nil)
					if broadcast {
						continue
					}
					logBuf.AssertContains(testutil.LogEntry{
						Level: zap.InfoLevel, Message: "local replica of the object is redundant in the container, removing...", Fields: map[string]any{
							"component": "Object Policer",
							"object":    objAddr.String(),
						},
					})
				}
			})
		})
		t.Run("out container", func(t *testing.T) {
			t.Run("in netmap", func(t *testing.T) {
				logBuf := testRepCheck(t, defaultRep, localObj, nodes, -1, true, allOK, 0, true, nil)
				logBuf.AssertContains(testutil.LogEntry{
					Level: zap.InfoLevel, Message: "node outside the container, removing the replica so as not to violate the storage policy...", Fields: map[string]any{
						"component": "Object Policer",
						"object":    objAddr.String(),
					},
				})
			})
			t.Run("out netmap", func(t *testing.T) {
				logBuf := testRepCheck(t, defaultRep, localObj, nodes, -1, false, allOK, 0, false, nil)
				logBuf.AssertContains(testutil.LogEntry{
					Level: zap.InfoLevel, Message: "node is outside the network map, holding the replica...", Fields: map[string]any{
						"component": "Object Policer",
						"object":    objAddr.String(),
					},
				})
			})
		})
	})

	t.Run("all 404", func(t *testing.T) {
		allNotFound := islices.RepeatElement(len(nodes), error(apistatus.ErrObjectNotFound))
		t.Run("in container", func(t *testing.T) {
			expShortage := uint32(defaultRep - 1)
			if broadcast {
				expShortage = uint32(len(nodes) - 1)
			}
			t.Run("primary", func(t *testing.T) {
				for i := range defaultRep {
					testRepCheck(t, defaultRep, localObj, nodes, i, true, allNotFound, expShortage, false, slices.Delete(slices.Clone(nodes), i, i+1))
				}
			})
			t.Run("backup", func(t *testing.T) {
				for i := defaultRep; i < len(nodes); i++ {
					testRepCheck(t, defaultRep, localObj, nodes, i, true, allNotFound, expShortage, !broadcast, slices.Delete(slices.Clone(nodes), i, i+1))
				}
			})
		})
		t.Run("out container", func(t *testing.T) {
			expShortage := uint32(defaultRep)
			if broadcast {
				expShortage = uint32(len(nodes))
			}
			t.Run("in netmap", func(t *testing.T) {
				logBuf := testRepCheck(t, defaultRep, localObj, nodes, -1, true, allNotFound, expShortage, true, nodes)
				logBuf.AssertContains(testutil.LogEntry{
					Level: zap.InfoLevel, Message: "node outside the container, removing the replica so as not to violate the storage policy...", Fields: map[string]any{
						"component": "Object Policer",
						"object":    objAddr.String(),
					},
				})
			})
			t.Run("out netmap", func(t *testing.T) {
				testRepCheck(t, defaultRep, localObj, nodes, -1, false, allNotFound, expShortage, false, nodes)
			})
		})
	})

	t.Run("maintenance", func(t *testing.T) {
		errs := islices.RepeatElement(len(nodes), error(apistatus.ErrObjectNotFound))
		for i := range defaultRep {
			errs[i] = apistatus.ErrNodeUnderMaintenance
		}
		t.Run("in container", func(t *testing.T) {
			t.Run("primary", func(t *testing.T) {
				expShortage := uint32(0)
				if broadcast {
					expShortage = uint32(len(nodes)) - 3
				}

				for i := range defaultRep {
					var expCandidates []netmap.NodeInfo
					if broadcast {
						expCandidates = nodes[defaultRep:]
					}

					logBuf := testRepCheck(t, defaultRep, localObj, nodes, i, true, errs, expShortage, false, expCandidates)

					for j := range defaultRep {
						if j == i {
							continue
						}
						logBuf.AssertContains(testutil.LogEntry{
							Level: zap.DebugLevel, Message: "consider node under maintenance as OK", Fields: map[string]any{
								"component": "Object Policer",
								"node":      hex.EncodeToString(nodes[j].PublicKey()),
							},
						})
					}

					if broadcast {
						logBuf.AssertContains(testutil.LogEntry{
							Level: zap.DebugLevel, Message: "shortage of object copies detected", Fields: map[string]any{
								"component": "Object Policer",
								"object":    objAddr.String(),
								"shortage":  json.Number("6"),
							},
						})
						continue
					}

					logBuf.AssertContains(testutil.LogEntry{
						Level: zap.DebugLevel, Message: "some of the copies are stored on nodes under maintenance, save local copy", Fields: map[string]any{
							"component": "Object Policer",
							"count":     json.Number("2"),
						},
					})
				}
			})
			t.Run("backup", func(t *testing.T) {
				expShortage := uint32(0)
				if broadcast {
					expShortage = uint32(len(nodes)) - 4
				}

				for i := defaultRep; i < len(nodes); i++ {
					var expCandidates []netmap.NodeInfo
					if broadcast {
						expCandidates = slices.Delete(slices.Clone(nodes), i, i+1)
						expCandidates = expCandidates[defaultRep:]
					}

					logBuf := testRepCheck(t, defaultRep, localObj, nodes, i, true, errs, expShortage, false, expCandidates)

					for j := range defaultRep {
						logBuf.AssertContains(testutil.LogEntry{
							Level: zap.DebugLevel, Message: "consider node under maintenance as OK", Fields: map[string]any{
								"component": "Object Policer",
								"node":      hex.EncodeToString(nodes[j].PublicKey()),
							},
						})
					}

					if broadcast {
						logBuf.AssertContains(testutil.LogEntry{
							Level: zap.DebugLevel, Message: "shortage of object copies detected", Fields: map[string]any{
								"component": "Object Policer",
								"object":    objAddr.String(),
								"shortage":  json.Number("5"),
							},
						})
						continue
					}

					logBuf.AssertContains(testutil.LogEntry{
						Level: zap.DebugLevel, Message: "some of the copies are stored on nodes under maintenance, save local copy", Fields: map[string]any{
							"component": "Object Policer",
							"count":     json.Number("3"),
						},
					})
				}
			})
		})
		t.Run("out container", func(t *testing.T) {
			t.Run("in netmap", func(t *testing.T) {
				expShortage := uint32(0)
				expRedundant := false
				if broadcast {
					expShortage = uint32(len(nodes)) - 3
					expRedundant = true
				}

				var expCandidates []netmap.NodeInfo
				if broadcast {
					expCandidates = nodes[defaultRep:]
				}

				logBuf := testRepCheck(t, defaultRep, localObj, nodes, -1, true, errs, expShortage, expRedundant, expCandidates)

				for j := range defaultRep {
					logBuf.AssertContains(testutil.LogEntry{
						Level: zap.DebugLevel, Message: "consider node under maintenance as OK", Fields: map[string]any{
							"component": "Object Policer",
							"node":      hex.EncodeToString(nodes[j].PublicKey()),
						},
					})
				}

				if broadcast {
					logBuf.AssertContains(testutil.LogEntry{
						Level: zap.DebugLevel, Message: "shortage of object copies detected", Fields: map[string]any{
							"component": "Object Policer",
							"object":    objAddr.String(),
							"shortage":  json.Number("6"),
						},
					})
					return
				}

				logBuf.AssertContains(testutil.LogEntry{
					Level: zap.DebugLevel, Message: "some of the copies are stored on nodes under maintenance, save local copy", Fields: map[string]any{
						"component": "Object Policer",
						"count":     json.Number("3"),
					},
				})
			})
			t.Run("out netmap", func(t *testing.T) {
				expShortage := uint32(0)
				if broadcast {
					expShortage = uint32(len(nodes)) - 3
				}

				var expCandidates []netmap.NodeInfo
				if broadcast {
					expCandidates = nodes[defaultRep:]
				}

				logBuf := testRepCheck(t, defaultRep, localObj, nodes, -1, false, errs, expShortage, false, expCandidates)

				for j := range defaultRep {
					logBuf.AssertContains(testutil.LogEntry{
						Level: zap.DebugLevel, Message: "consider node under maintenance as OK", Fields: map[string]any{
							"component": "Object Policer",
							"node":      hex.EncodeToString(nodes[j].PublicKey()),
						},
					})
				}

				if broadcast {
					logBuf.AssertContains(testutil.LogEntry{
						Level: zap.DebugLevel, Message: "shortage of object copies detected", Fields: map[string]any{
							"component": "Object Policer",
							"object":    objAddr.String(),
							"shortage":  json.Number("6"),
						},
					})
					return
				}

				logBuf.AssertContains(testutil.LogEntry{
					Level: zap.DebugLevel, Message: "some of the copies are stored on nodes under maintenance, save local copy", Fields: map[string]any{
						"component": "Object Policer",
						"count":     json.Number("3"),
					},
				})
			})
		})
	})

	t.Run("various errors", func(t *testing.T) {
		t.Run("in container", func(t *testing.T) {
			t.Run("primary", func(t *testing.T) {
				errs := islices.RepeatElement(len(nodes), error(apistatus.ErrObjectNotFound))
				errs[0] = errors.New("non-status error")
				errs[1] = nil

				expShortage := uint32(1)
				if broadcast {
					expShortage = uint32(len(nodes)) - 2
				}

				logBuf := testRepCheck(t, defaultRep, localObj, nodes, 2, true, errs, expShortage, false, slices.Clone(nodes)[3:])
				e := testutil.LogEntry{
					Level:   zap.ErrorLevel,
					Message: "receive object header to check policy compliance",
					Fields: map[string]any{
						"component": "Object Policer",
						"object":    objAddr.String(),
						"error":     "non-status error",
					},
				}
				logBuf.AssertContains(e)

				errs[1] = apistatus.ErrServerInternal

				expShortage = 2
				if broadcast {
					expShortage = uint32(len(nodes)) - 1
				}

				logBuf = testRepCheck(t, defaultRep, localObj, nodes, defaultRep-1, true, errs, expShortage, false, slices.Clone(nodes)[defaultRep:])
				e.Fields["object"] = objAddr.String()
				logBuf.AssertContains(e)
				e.Fields["error"] = errs[1].Error()
				logBuf.AssertContains(e)
			})
			t.Run("backup", func(t *testing.T) {
				errs := islices.RepeatElement(len(nodes), error(apistatus.ErrObjectNotFound))
				errs[0] = errors.New("non-status error")
				errs[1] = nil
				errs[2] = nil

				expShortage := uint32(0)
				if broadcast {
					expShortage = uint32(len(nodes)) - 3
				}

				logBuf := testRepCheck(t, defaultRep, localObj, nodes, defaultRep, true, errs, expShortage, false, slices.Clone(nodes)[defaultRep+1:])
				e := testutil.LogEntry{
					Level:   zap.ErrorLevel,
					Message: "receive object header to check policy compliance",
					Fields: map[string]any{
						"component": "Object Policer",
						"object":    objAddr.String(),
						"error":     "non-status error",
					},
				}
				logBuf.AssertContains(e)

				errs[2] = apistatus.ErrObjectAlreadyRemoved

				expShortage = 1
				if broadcast {
					expShortage = uint32(len(nodes)) - 2
				}

				candidates := slices.Clone(nodes)[defaultRep:]
				candidates = slices.Delete(candidates, 1, 2)

				logBuf = testRepCheck(t, defaultRep, localObj, nodes, defaultRep+1, true, errs, expShortage, false, candidates)
				e.Fields["object"] = objAddr.String()
				logBuf.AssertContains(e)
				e.Fields["error"] = errs[2].Error()
				logBuf.AssertContains(e)
			})
		})
		t.Run("out container", func(t *testing.T) {
			t.Run("in netmap", func(t *testing.T) {
				errs := islices.RepeatElement(len(nodes), error(apistatus.ErrObjectNotFound))
				errs[0] = errors.New("non-status error")
				errs[1] = nil
				errs[2] = nil

				expShortage := uint32(1)
				if broadcast {
					expShortage = uint32(len(nodes)) - 2
				}

				logBuf := testRepCheck(t, defaultRep, localObj, nodes, -1, true, errs, expShortage, true, slices.Clone(nodes)[3:])
				e := testutil.LogEntry{
					Level:   zap.ErrorLevel,
					Message: "receive object header to check policy compliance",
					Fields: map[string]any{
						"component": "Object Policer",
						"object":    objAddr.String(),
						"error":     "non-status error",
					},
				}
				logBuf.AssertContains(e)

				errs[2] = apistatus.ErrServerInternal

				expShortage = uint32(2)
				if broadcast {
					expShortage = uint32(len(nodes)) - 1
				}

				logBuf = testRepCheck(t, defaultRep, localObj, nodes, -1, true, errs, expShortage, true, slices.Clone(nodes)[defaultRep:])
				e.Fields["object"] = objAddr.String()
				logBuf.AssertContains(e)
				e.Fields["error"] = errs[2].Error()
				logBuf.AssertContains(e)
			})
			t.Run("out netmap", func(t *testing.T) {
				errs := islices.RepeatElement(len(nodes), error(apistatus.ErrObjectNotFound))
				errs[0] = errors.New("non-status error")
				errs[1] = nil
				errs[2] = nil

				expShortage := uint32(1)
				if broadcast {
					expShortage = uint32(len(nodes)) - 2
				}

				logBuf := testRepCheck(t, defaultRep, localObj, nodes, -1, false, errs, expShortage, false, slices.Clone(nodes)[3:])
				e := testutil.LogEntry{
					Level:   zap.ErrorLevel,
					Message: "receive object header to check policy compliance",
					Fields: map[string]any{
						"component": "Object Policer",
						"object":    objAddr.String(),
						"error":     "non-status error",
					},
				}
				logBuf.AssertContains(e)

				errs[2] = apistatus.ErrServerInternal

				expShortage = uint32(2)
				if broadcast {
					expShortage = uint32(len(nodes)) - 1
				}

				logBuf = testRepCheck(t, defaultRep, localObj, nodes, -1, false, errs, expShortage, false, slices.Clone(nodes)[defaultRep:])
				e.Fields["object"] = objAddr.String()
				logBuf.AssertContains(e)
				e.Fields["error"] = errs[2].Error()
				logBuf.AssertContains(e)
			})
		})
	})

	t.Run("misplaced", func(t *testing.T) {
		if broadcast {
			// For LOCK/LINK shortage == len(nodes) so it never reaches 0 early
			return
		}

		t.Run("one misplaced primary / in container", func(t *testing.T) {
			// local=node[0], node[1]=404, node[2]=nil, rest=nil
			// misplacedCandidates=[node[1]]
			errs := islices.RepeatElement(len(nodes), error(nil))
			errs[1] = apistatus.ErrObjectNotFound

			expCandidates := []netmap.NodeInfo{nodes[1]}
			logBuf := testRepCheck(t, defaultRep, localObj, nodes, 0, true, errs, 1, false, expCandidates)
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.DebugLevel, Message: "misplaced object copies detected, replicating to primary nodes", Fields: map[string]any{
					"component": "Object Policer",
					"object":    objAddr.String(),
					"misplaced": json.Number("1"),
				},
			})
		})

		t.Run("two misplaced primaries / in container", func(t *testing.T) {
			// local=node[0], nodes[1]=404, nodes[2]=404, nodes[3]=nil, nodes[4]=nil
			// misplacedCandidates=[node[1], node[2]]
			errs := islices.RepeatElement(len(nodes), error(nil))
			errs[1] = apistatus.ErrObjectNotFound
			errs[2] = apistatus.ErrObjectNotFound

			expCandidates := []netmap.NodeInfo{nodes[1], nodes[2]}
			logBuf := testRepCheck(t, defaultRep, localObj, nodes, 0, true, errs, 2, false, expCandidates)
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.DebugLevel, Message: "misplaced object copies detected, replicating to primary nodes", Fields: map[string]any{
					"component": "Object Policer",
					"object":    objAddr.String(),
					"misplaced": json.Number("2"),
				},
			})
		})

		t.Run("one misplaced primary / out container", func(t *testing.T) {
			// local=-1 (not in nodes), nodes[0]=404, nodes[1]=nil, nodes[2]=nil
			// misplacedCandidates=[node[0]]
			errs := islices.RepeatElement(len(nodes), error(nil))
			errs[0] = apistatus.ErrObjectNotFound

			expCandidates := []netmap.NodeInfo{nodes[0]}
			logBuf := testRepCheck(t, defaultRep, localObj, nodes, -1, true, errs, 1, true, expCandidates)
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.DebugLevel, Message: "misplaced object copies detected, replicating to primary nodes", Fields: map[string]any{
					"component": "Object Policer",
					"object":    objAddr.String(),
					"misplaced": json.Number("1"),
				},
			})
		})
	})
}

func testRepCheck(t *testing.T, rep uint, localObj objectcore.AddressWithAttributes, nodes []netmap.NodeInfo, localIdx int, localInNM bool, headErrs []error,
	expShortage uint32, expRedundant bool, expCandidates []netmap.NodeInfo) *testutil.LogBuffer {
	require.Equal(t, len(nodes), len(headErrs))

	localNode := newTestLocalNode()
	localNode.objList = []objectcore.AddressWithAttributes{localObj}

	mockNet := newMockNetwork()
	mockNet.setObjectNodesRepResult(localObj.Address.Container(), localObj.Address.Object(), slices.Clone(nodes), rep)
	if localIdx >= 0 {
		mockNet.pubKey = nodes[localIdx].PublicKey()
		mockNet.inNetmap = true
	} else {
		mockNet.inNetmap = localInNM
	}

	conns := newMockAPIConnections()
	for i := range nodes {
		if i != localIdx {
			conns.setHeadResult(nodes[i], localObj.Address, headErrs[i])
		}
	}

	r := newTestReplicator(t)
	r.success = true
	if expShortage > 0 {
		r.successfulCopies = expShortage
		r.onTask = func(_ replicator.Task, successfulNodes []netmap.NodeInfo) {
			for _, node := range successfulNodes {
				conns.setHeadResult(node, localObj.Address, nil)
			}
		}
	}

	l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)
	p := New(neofscryptotest.Signer(),
		WithReplicationCooldown(50*time.Millisecond),
		WithNetwork(mockNet),
		WithLogger(l),
		WithMetrics(&mockMetrics{}),
	)
	p.localStorage = localNode
	p.apiConns = conns
	p.replicator = r

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go p.Run(ctx)

		time.Sleep(time.Second)
		waitForPolicerResult(t, p, lb, mockNet, localNode, localObj.Address, expRedundant, expShortage > 0, r)
	})
	var taskV = r.task.Load()
	if expShortage > 0 {
		require.NotNil(t, taskV)

		var exp replicator.Task
		exp.SetObjectAddress(localObj.Address)
		exp.SetCopiesNumber(expShortage)
		exp.SetNodes(expCandidates)
		require.Equal(t, exp, taskV.(replicator.Task))
	} else {
		require.Nil(t, taskV)
	}

	if expRedundant {
		require.Equal(t, []oid.Address{localObj.Address}, localNode.deletedObjects())
	} else {
		require.Empty(t, localNode.deletedObjects())
	}

	return lb
}

func TestPolicer_Run_EC(t *testing.T) {
	const defaultCBF = 3
	cnr := cidtest.ID()
	partOID := oidtest.ID()
	parentOID := oidtest.OtherID(partOID)
	parentOIDAttr := string(parentOID[:])
	rule := iec.Rule{
		DataPartNum:   6,
		ParityPartNum: 3,
	}
	localObj := objectcore.AddressWithAttributes{
		Address:    oid.NewAddress(cnr, partOID),
		Type:       object.TypeRegular,
		Attributes: []string{"0", "5", parentOIDAttr},
	}

	nodes := testutil.Nodes(int(rule.DataPartNum+rule.ParityPartNum) * defaultCBF)
	allOK := islices.RepeatElement(len(nodes), error(nil))
	all404 := islices.RepeatElement(len(nodes), error(apistatus.ErrObjectNotFound))
	optimalOrder := []int{
		5, 14, 23,
		6, 15, 24,
		7, 16, 25,
		8, 17, 26,
		0, 9, 18,
		1, 10, 19,
		2, 11, 20,
		3, 12, 21,
		4, 13, 22,
	}

	t.Run("invalid EC attributes in local object", func(t *testing.T) {
		localObj := localObj

		for _, tc := range []struct {
			name    string
			ruleIdx string
			partIdx string
			err     string
		}{
			{name: "non-int rule index", ruleIdx: "not_an_int", partIdx: "34",
				err: `decode rule index: strconv.ParseUint: parsing "not_an_int": invalid syntax`},
			{name: "negative rule index", ruleIdx: "-12", partIdx: "34",
				err: `decode rule index: strconv.ParseUint: parsing "-12": invalid syntax`},
			{name: "rule index overflow", ruleIdx: "256", partIdx: "34",
				err: "rule index out of range"},
			{name: "non-int part index", ruleIdx: "12", partIdx: "not_an_int",
				err: `decode part index: strconv.ParseUint: parsing "not_an_int": invalid syntax`},
			{name: "negative part index", ruleIdx: "12", partIdx: "-34",
				err: `decode part index: strconv.ParseUint: parsing "-34": invalid syntax`},
			{name: "part index overflow", ruleIdx: "12", partIdx: "256",
				err: "part index out of range"},
			{name: "rule index without part index", ruleIdx: "12", partIdx: "",
				err: "rule index is set, part index is not"},
			{name: "part index without rule index", ruleIdx: "", partIdx: "34",
				err: "part index is set, rule index is not"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				localObj.Attributes = []string{tc.ruleIdx, tc.partIdx, parentOIDAttr}

				testECCheckWaitLog(t, rule, localObj, nodes, 0, allOK, testutil.LogEntry{
					Level: zap.ErrorLevel, Message: "failed to decode EC part info from attributes, skip object",
					Fields: map[string]any{"component": "Object Policer", "object": localObj.Address.String(), "error": tc.err},
				})
			})
		}
	})

	t.Run("invalid parent attribute in local object", func(t *testing.T) {
		localObj := localObj
		localObj.Attributes = slices.Clone(localObj.Attributes)
		localObj.Attributes[2] = string(testutil.RandByteSlice(31))

		mockNet := newMockNetwork()
		mockNet.setObjectNodesECResult(cnr, parentOID, nodes, rule)

		testECCheckWithNetworkAndShortage(t, mockNet, localObj, nodes, 0, allOK, false, nil, false, 0, testutil.LogEntry{
			Level: zap.ErrorLevel, Message: "received EC parent OID with unexpected len from local storage, skip object",
			Fields: map[string]any{"component": "Object Policer", "object": localObj.Address.String(), "len": json.Number("31")},
		})
	})

	t.Run("EC part in non-EC container", func(t *testing.T) {
		mockNet := newMockNetwork()
		mockNet.setObjectNodesRepResult(localObj.Address.Container(), parentOID, nodes, 3)

		logBuf := testECCheckWithNetwork(t, mockNet, localObj, nodes, 0, allOK, true, nil)
		logBuf.AssertContains(testutil.LogEntry{
			Level: zap.InfoLevel, Message: "object with EC attributes in container without EC rules detected, deleting",
			Fields: map[string]any{"component": "Object Policer", "object": localObj.Address.String(),
				"ruleIdx": json.Number(localObj.Attributes[0]), "partIdx": json.Number(localObj.Attributes[1])},
		})
	})

	t.Run("part violates EC policy", func(t *testing.T) {
		t.Run("no EC attributes", func(t *testing.T) {
			localObj := localObj
			localObj.Attributes = []string{"", "", parentOIDAttr}

			logBuf := testECCheck(t, rule, localObj, nodes, 0, allOK, true, nil)
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.InfoLevel, Message: "object with lacking EC attributes detected, deleting",
				Fields: map[string]any{"component": "Object Policer", "object": localObj.Address.String()},
			})
		})
		t.Run("too big rule index", func(t *testing.T) {
			localObj := localObj
			localObj.Attributes = slices.Clone(localObj.Attributes)
			localObj.Attributes[0] = "1"

			logBuf := testECCheck(t, rule, localObj, nodes, 0, allOK, true, nil)
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.WarnLevel, Message: "local object with invalid EC rule index detected, deleting",
				Fields: map[string]any{"component": "Object Policer", "object": localObj.Address.String(),
					"ruleIdx": json.Number("1"), "totalRules": json.Number("1")},
			})
		})
		t.Run("too big part index", func(t *testing.T) {
			rule := iec.Rule{DataPartNum: 17, ParityPartNum: 4}
			localObj := localObj
			localObj.Attributes = slices.Clone(localObj.Attributes)
			localObj.Attributes[1] = "21"

			logBuf := testECCheck(t, rule, localObj, nodes, 0, allOK, true, nil)
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.WarnLevel, Message: "local object with invalid EC part index detected, deleting",
				Fields: map[string]any{"component": "Object Policer", "object": localObj.Address.String(), "rule": "17/4",
					"partIdx": json.Number("21")},
			})
		})
	})

	t.Run("local node is optimal", func(t *testing.T) {
		logBuf := testECCheck(t, rule, localObj, nodes, 5, all404, false, nil)
		logBuf.AssertContains(testutil.LogEntry{
			Level: zap.DebugLevel, Message: "local node is optimal for EC part, hold",
			Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
				"rule": "6/3", "partIdx": json.Number("5")},
		})
	})

	t.Run("found on more optimal node", func(t *testing.T) {
		errs := slices.Clone(all404)
		errs[5] = nil

		logBuf := testECCheck(t, rule, localObj, nodes, 6, errs, true, nil)
		logBuf.AssertContains(testutil.LogEntry{
			Level: zap.InfoLevel, Message: "EC part header successfully received from more optimal node, drop",
			Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
				"rule": "6/3", "partIdx": json.Number("5"), "node": []any{"localhost:10010", "localhost:10011"}},
		})
	})

	t.Run("not found on more optimal node", func(t *testing.T) {
		candidates := islices.CollectIndex(nodes, optimalOrder[:len(optimalOrder)-1]...)

		t.Run("move failure", func(t *testing.T) {
			logBuf := testECCheck(t, rule, localObj, nodes, optimalOrder[len(optimalOrder)-1], all404, false, candidates)
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.InfoLevel, Message: "local node is suboptimal for EC part, moving to more optimal node...",
				Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
					"rule": "6/3", "partIdx": json.Number("5"), "candidateNum": json.Number("26")},
			})
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.InfoLevel, Message: "failed to move EC part to more optimal node, hold",
				Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
					"rule": "6/3", "partIdx": json.Number("5"), "candidateNum": json.Number("26")},
			})
		})

		logBuf := testECCheck(t, rule, localObj, nodes, optimalOrder[len(optimalOrder)-1], all404, true, candidates)
		logBuf.AssertContains(testutil.LogEntry{
			Level: zap.InfoLevel, Message: "local node is suboptimal for EC part, moving to more optimal node...",
			Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(), "rule": "6/3",
				"partIdx": json.Number("5"), "candidateNum": json.Number("26")},
		})
		logBuf.AssertContains(testutil.LogEntry{
			Level: zap.InfoLevel, Message: "EC part successfully moved to more optimal node, drop",
			Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(), "rule": "6/3",
				"partIdx": json.Number("5"), "newHolder": []any{"localhost:10010", "localhost:10011"}},
		})
	})

	t.Run("maintenance", func(t *testing.T) {
		errs := slices.Clone(all404)
		errs[optimalOrder[len(optimalOrder)-3]] = apistatus.ErrNodeUnderMaintenance

		t.Run("moved", func(t *testing.T) {
			errs := slices.Clone(errs)
			errs[optimalOrder[len(optimalOrder)-2]] = nil

			logBuf := testECCheck(t, rule, localObj, nodes, optimalOrder[len(optimalOrder)-1], errs, true, nil)
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.InfoLevel, Message: "failed to receive EC part header from more optimal node due to its maintenance, continue",
				Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
					"rule": "6/3", "partIdx": json.Number("5"), "node": []any{"localhost:10008", "localhost:10009"}},
			})
		})

		logBuf := testECCheck(t, rule, localObj, nodes, optimalOrder[len(optimalOrder)-1], errs, false, nil)
		logBuf.AssertContains(testutil.LogEntry{
			Level: zap.InfoLevel, Message: "failed to receive EC part header from more optimal node due to its maintenance, continue",
			Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
				"rule": "6/3", "partIdx": json.Number("5"), "node": []any{"localhost:10008", "localhost:10009"}},
		})
	})

	t.Run("various errors", func(t *testing.T) {
		otherErrs := []error{
			errors.New("some error"),
			apistatus.ErrServerInternal,
			apistatus.ErrWrongMagicNumber,
			apistatus.ErrSignatureVerification,
			apistatus.ErrObjectAccessDenied,
			apistatus.ErrObjectAlreadyRemoved,
			apistatus.ErrContainerNotFound,
			apistatus.ErrSessionTokenExpired,
		}

		errs := slices.Clone(all404)
		for i := range otherErrs {
			errs[optimalOrder[i]] = otherErrs[i]
		}

		checkErrsLog := func(logBuf *testutil.LogBuffer) {
			for i := range otherErrs {
				logBuf.AssertContains(testutil.LogEntry{
					Level: zap.InfoLevel, Message: "failed to receive EC part header from more optimal node, exclude",
					Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(), "rule": "6/3",
						"partIdx": json.Number("5"), "error": otherErrs[i].Error()},
				})
			}
		}

		holderField := []any{"localhost:10050", "localhost:10051"}

		testWithInContainer := func(t *testing.T, inCnr bool) {
			candidates := islices.CollectIndex(nodes, optimalOrder[len(otherErrs):]...)
			localIdx := -1
			candidateNum := len(nodes) - len(otherErrs)
			if inCnr {
				localIdx = optimalOrder[len(optimalOrder)-1]
				candidates = candidates[:len(candidates)-1]
				candidateNum--
			}

			candidateNumField := json.Number(strconv.Itoa(candidateNum))

			t.Run("drop", func(t *testing.T) {
				errs := slices.Clone(errs)
				errs[optimalOrder[len(otherErrs)]] = nil

				logBuf := testECCheck(t, rule, localObj, nodes, localIdx, errs, true, nil)
				checkErrsLog(logBuf)
				logBuf.AssertContains(testutil.LogEntry{
					Level: zap.InfoLevel, Message: "EC part header successfully received from more optimal node, drop",
					Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
						"rule": "6/3", "partIdx": json.Number("5"), "node": holderField},
				})
			})

			t.Run("move failure", func(t *testing.T) {
				logBuf := testECCheck(t, rule, localObj, nodes, localIdx, errs, false, candidates)
				checkErrsLog(logBuf)
				logBuf.AssertContains(testutil.LogEntry{
					Level: zap.InfoLevel, Message: "local node is suboptimal for EC part, moving to more optimal node...",
					Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
						"rule": "6/3", "partIdx": json.Number("5"), "candidateNum": candidateNumField},
				})
				logBuf.AssertContains(testutil.LogEntry{
					Level: zap.InfoLevel, Message: "failed to move EC part to more optimal node, hold",
					Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
						"rule": "6/3", "partIdx": json.Number("5"), "candidateNum": candidateNumField},
				})
			})

			logBuf := testECCheck(t, rule, localObj, nodes, localIdx, errs, true, candidates)
			checkErrsLog(logBuf)
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.InfoLevel, Message: "local node is suboptimal for EC part, moving to more optimal node...",
				Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
					"rule": "6/3", "partIdx": json.Number("5"), "candidateNum": candidateNumField},
			})
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.InfoLevel, Message: "EC part successfully moved to more optimal node, drop",
				Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
					"rule": "6/3", "partIdx": json.Number("5"), "newHolder": holderField},
			})
		}

		t.Run("in container", func(t *testing.T) {
			testWithInContainer(t, true)
		})
		t.Run("out container", func(t *testing.T) {
			testWithInContainer(t, false)
		})
	})

	for _, typ := range []object.Type{
		object.TypeTombstone,
		object.TypeLock,
		object.TypeLink,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			localObj := localObj
			localObj.Type = typ
			localObj.Attributes = make([]string, 3)

			for localIdx := -1; localIdx < len(nodes); localIdx++ {
				mockNet := newMockNetwork()
				mockNet.setObjectNodesECResult(cnr, partOID, slices.Clone(nodes), rule)
				if localIdx >= 0 {
					mockNet.pubKey = nodes[localIdx].PublicKey()
				}

				expShortage := uint32(len(nodes))
				expShortageStr := strconv.FormatUint(uint64(expShortage), 10)
				expCandidates := nodes
				if localIdx >= 0 {
					expShortage--
					expShortageStr = strconv.FormatUint(uint64(expShortage), 10)
					expCandidates = slices.Clone(nodes)
					expCandidates = slices.Delete(expCandidates, localIdx, localIdx+1)
				}

				logBuf := testECCheckWithNetworkAndShortage(t, mockNet, localObj, nodes, localIdx, all404, false, expCandidates, true, expShortage)
				logBuf.AssertContains(testutil.LogEntry{
					Level: zap.DebugLevel, Message: "shortage of object copies detected", Fields: map[string]any{
						"component": "Object Policer",
						"object":    localObj.Address.String(),
						"shortage":  json.Number(expShortageStr),
					},
				})
			}
		})
	}
}

func newECMockNetwork(t *testing.T, rule iec.Rule, localObj objectcore.AddressWithAttributes, nodes []netmap.NodeInfo, localIdx int) *mockNetwork {
	require.Len(t, localObj.Attributes, 3)

	var sortOID oid.ID
	if localObj.Attributes[0] == "" && localObj.Attributes[1] == "" {
		sortOID = localObj.Address.Object()
	} else {
		require.Len(t, localObj.Attributes[2], oid.Size)
		sortOID = oid.ID([]byte(localObj.Attributes[2]))
	}

	mockNet := newMockNetwork()
	mockNet.setObjectNodesECResult(localObj.Address.Container(), sortOID, slices.Clone(nodes), rule)
	if localIdx >= 0 {
		mockNet.pubKey = nodes[localIdx].PublicKey()
	}

	return mockNet
}

func testECCheck(t *testing.T, rule iec.Rule, localObj objectcore.AddressWithAttributes, nodes []netmap.NodeInfo, localIdx int, headErrs []error, expRedundant bool, expCandidates []netmap.NodeInfo) *testutil.LogBuffer {
	mockNet := newECMockNetwork(t, rule, localObj, nodes, localIdx)
	return testECCheckWithNetwork(t, mockNet, localObj, nodes, localIdx, headErrs, expRedundant, expCandidates)
}

func testECCheckWaitLog(t *testing.T, rule iec.Rule, localObj objectcore.AddressWithAttributes, nodes []netmap.NodeInfo, localIdx int, headErrs []error, exp testutil.LogEntry) {
	mockNet := newECMockNetwork(t, rule, localObj, nodes, localIdx)
	testECCheckWithNetworkAndShortage(t, mockNet, localObj, nodes, localIdx, headErrs, false, nil, false, 0, exp)
}

func testECCheckWithNetwork(t *testing.T, mockNet *mockNetwork, localObj objectcore.AddressWithAttributes, nodes []netmap.NodeInfo,
	localIdx int, headErrs []error, expRedundant bool, expCandidates []netmap.NodeInfo) *testutil.LogBuffer {
	expShortage := uint32(0)
	if len(expCandidates) > 0 {
		expShortage = 1
	}

	return testECCheckWithNetworkAndShortage(t, mockNet, localObj, nodes, localIdx, headErrs, expRedundant, expCandidates, expRedundant, expShortage)
}

func testECCheckWithNetworkAndShortage(t *testing.T, mockNet *mockNetwork, localObj objectcore.AddressWithAttributes, nodes []netmap.NodeInfo,
	localIdx int, headErrs []error, expRedundant bool, expCandidates []netmap.NodeInfo, repSuccess bool, expShortage uint32, waitLog ...testutil.LogEntry) *testutil.LogBuffer {
	require.Equal(t, len(nodes), len(headErrs))

	localNode := newTestLocalNode()
	localNode.objList = []objectcore.AddressWithAttributes{localObj}

	conns := newMockAPIConnections()
	for i := range nodes {
		if i != localIdx {
			conns.setHeadResult(nodes[i], localObj.Address, headErrs[i])
		}
	}

	r := newTestReplicator(t)
	r.success = repSuccess
	if repSuccess && expShortage > 0 {
		r.successfulCopies = expShortage
		r.onTask = func(_ replicator.Task, successfulNodes []netmap.NodeInfo) {
			for _, node := range successfulNodes {
				conns.setHeadResult(node, localObj.Address, nil)
			}
		}
	}

	l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)
	p := New(neofscryptotest.Signer(),
		WithReplicationCooldown(50*time.Millisecond), // any huge time to cancel process repeat
		WithNetwork(mockNet),
		WithLogger(l),
		WithMetrics(&mockMetrics{}),
	)
	p.localStorage = localNode
	p.apiConns = conns
	p.replicator = r

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go p.Run(ctx)

		var expLog *testutil.LogEntry
		if len(waitLog) > 0 {
			expLog = &waitLog[0]
		}

		time.Sleep(time.Second)
		waitForPolicerResult(t, p, lb, mockNet, localNode, localObj.Address, expRedundant, len(expCandidates) > 0, r, expLog)
		synctest.Wait()
	})
	var taskV = r.task.Load()
	if len(expCandidates) > 0 {
		require.NotNil(t, taskV)

		var exp replicator.Task
		exp.SetObjectAddress(localObj.Address)
		exp.SetCopiesNumber(expShortage)
		exp.SetNodes(expCandidates)
		require.Equal(t, exp, taskV.(replicator.Task))
	} else {
		require.Nil(t, taskV)
	}

	if expRedundant {
		require.Equal(t, []oid.Address{localObj.Address}, localNode.deletedObjects())
	} else {
		require.Empty(t, localNode.deletedObjects())
	}

	return lb
}

func TestPolicer_DropShardDuplicates(t *testing.T) {
	t.Run("regular", func(t *testing.T) {
		cnr := cidtest.ID()
		objID := oidtest.ID()
		addr := oid.NewAddress(cnr, objID)
		nodes := testutil.Nodes(2)

		localObj := objectcore.AddressWithAttributes{
			Address:    addr,
			Type:       object.TypeRegular,
			Attributes: make([]string, 3),
			ShardIDs:   []string{"redundant", "keeper"},
		}

		localNode := newTestLocalNode()
		localNode.deleteRedundantCopies = func(got oid.Address, shardIDs []string) error {
			require.Equal(t, addr, got)
			require.ElementsMatch(t, []string{"redundant", "keeper"}, shardIDs)
			localNode.delMtx.Lock()
			localNode.delByShard[addr] = []string{"redundant"}
			localNode.delMtx.Unlock()
			return nil
		}

		mockNet := newMockNetwork()
		mockNet.pubKey = nodes[0].PublicKey()
		mockNet.setObjectNodesRepResult(cnr, objID, nodes, 1)

		p := New(neofscryptotest.Signer(),
			WithNetwork(mockNet),
			WithLogger(zap.NewNop()),
		)
		p.localStorage = localNode

		p.processObject(context.Background(), localObj)

		require.Empty(t, localNode.deletedObjects())
		require.Equal(t, []string{"redundant"}, localNode.deletedShardCopies(addr))
	})

	t.Run("broadcast", func(t *testing.T) {
		for _, typ := range []object.Type{object.TypeTombstone, object.TypeLock, object.TypeLink} {
			t.Run(typ.String(), func(t *testing.T) {
				cnr := cidtest.ID()
				objID := oidtest.ID()
				addr := oid.NewAddress(cnr, objID)
				nodes := testutil.Nodes(2)

				localObj := objectcore.AddressWithAttributes{
					Address:    addr,
					Type:       typ,
					Attributes: make([]string, 3),
					ShardIDs:   []string{"A", "B"},
				}

				localNode := newTestLocalNode()
				localNode.deleteRedundantCopies = func(oid.Address, []string) error {
					t.Fatal("DeleteRedundantCopies must not be called for broadcast objects")
					return nil
				}

				mockNet := newMockNetwork()
				mockNet.pubKey = nodes[0].PublicKey()
				mockNet.setObjectNodesRepResult(cnr, objID, nodes, 1)
				conns := newMockAPIConnections()
				conns.setHeadResult(nodes[1], addr, nil)

				p := New(neofscryptotest.Signer(),
					WithNetwork(mockNet),
					WithLogger(zap.NewNop()),
				)
				p.localStorage = localNode
				p.apiConns = conns

				p.processObject(context.Background(), localObj)

				require.Empty(t, localNode.deletedObjects())
				require.Empty(t, localNode.deletedShardCopies(addr))
			})
		}
	})
}

func waitForPolicerResult(t *testing.T, p *Policer, lb *testutil.LogBuffer, mockNet *mockNetwork, localNode *testLocalNode, addr oid.Address, expRedundant bool, expectTask bool, r *testReplicator, waitLog ...*testutil.LogEntry) {
	t.Helper()

	var expLog *testutil.LogEntry
	if len(waitLog) > 0 {
		expLog = waitLog[0]
	}

	if expRedundant {
		require.Equal(t, []oid.Address{addr}, localNode.deletedObjects())
		return
	}

	if expectTask {
		require.NotNil(t, r.task.Load())
		return
	}

	if expLog != nil {
		require.True(t, lb.Contains(*expLog))
		return
	}

	require.Greater(t, mockNet.totalGetNodesCalls(), uint64(0))
}

type testReplicator struct {
	t                *testing.T
	task             atomic.Value
	success          bool
	successfulCopies uint32
	onTask           func(replicator.Task, []netmap.NodeInfo)
}

func newTestReplicator(t *testing.T) *testReplicator {
	return &testReplicator{
		t: t,
	}
}

func (x *testReplicator) HandleTask(ctx context.Context, task replicator.Task, r replicator.TaskResult) {
	require.NotNil(x.t, ctx)
	require.NotNil(x.t, r)

	nodes := task.Nodes()

	var successfulNodes []netmap.NodeInfo
	if x.success && len(nodes) > 0 {
		copies := x.successfulCopies
		if copies == 0 {
			copies = 1
		}
		if copies > uint32(len(nodes)) {
			copies = uint32(len(nodes))
		}

		successfulNodes = append(successfulNodes, nodes[:copies]...)
		for i := range successfulNodes {
			r.SubmitSuccessfulReplication(successfulNodes[i])
		}
	}
	if x.onTask != nil {
		x.onTask(task, successfulNodes)
	}

	// Prevent collisions on subsequent iterations
	_ = x.task.CompareAndSwap(nil, task)
}

type testLocalNode struct {
	objList []objectcore.AddressWithAttributes

	delMtx                sync.RWMutex
	del                   map[oid.Address]struct{}
	delByShard            map[oid.Address][]string
	deleteRedundantCopies func(oid.Address, []string) error
}

func newTestLocalNode() *testLocalNode {
	return &testLocalNode{
		del:        make(map[oid.Address]struct{}),
		delByShard: make(map[oid.Address][]string),
	}
}

type mockNetwork struct {
	pubKey []byte

	inNetmap bool

	mtx            sync.RWMutex
	getNodes       map[getNodesKey]getNodesValue
	getNodesCalled map[getNodesKey]uint64
}

func newMockNetwork() *mockNetwork {
	return &mockNetwork{
		getNodes:       make(map[getNodesKey]getNodesValue),
		getNodesCalled: make(map[getNodesKey]uint64),
	}
}

type mockMetrics struct {
	consistency      atomic.Bool
	optimalPlacement atomic.Bool
	cycleCount       atomic.Uint64
	processedRep     atomic.Uint64
	processedEC      atomic.Uint64
	replicatedRep    atomic.Uint64
	replicatedEC     atomic.Uint64
	deletedRep       atomic.Uint64
	deletedEC        atomic.Uint64
}

func (m *mockMetrics) SetPolicerConsistency(b bool) {
	m.consistency.Store(b)
}

func (m *mockMetrics) SetPolicerOptimalPlacement(b bool) {
	m.optimalPlacement.Store(b)
}

func (m *mockMetrics) IncPolicerCycleCount() {
	m.cycleCount.Add(1)
}

func (m *mockMetrics) IncPolicerObjectProcessed(isEC bool) {
	if isEC {
		m.processedEC.Add(1)
		return
	}
	m.processedRep.Add(1)
}

func (m *mockMetrics) IncPolicerObjectReplicated(isEC bool) {
	if isEC {
		m.replicatedEC.Add(1)
		return
	}
	m.replicatedRep.Add(1)
}

func (m *mockMetrics) IncPolicerObjectDeleted(isEC bool) {
	if isEC {
		m.deletedEC.Add(1)
		return
	}
	m.deletedRep.Add(1)
}

func (x *mockNetwork) IsLocalNodePublicKey(key []byte) bool {
	return bytes.Equal(key, x.pubKey)
}

func (x *mockNetwork) IsLocalNodeInNetmap() bool {
	return x.inNetmap
}

func (x *mockNetwork) PublicKey() []byte {
	return x.pubKey
}

func (x *testLocalNode) ListWithCursor(_ uint32, c *engine.Cursor, _ ...string) ([]objectcore.AddressWithAttributes, *engine.Cursor, error) {
	if len(x.objList) == 0 {
		return nil, nil, engine.ErrEndOfListing
	}

	x.delMtx.RLock()
	withoutDel := make([]objectcore.AddressWithAttributes, 0, len(x.objList))
	for _, obj := range x.objList {
		if _, ok := x.del[obj.Address]; !ok {
			withoutDel = append(withoutDel, obj)
		}
	}
	x.delMtx.RUnlock()

	if len(withoutDel) == 0 {
		return nil, nil, engine.ErrEndOfListing
	}

	if c == nil {
		lastObj := withoutDel[len(withoutDel)-1]
		return withoutDel, engine.NewCursor(lastObj.Address.Container(), lastObj.Address.Object()), nil
	}
	cursorAddr := oid.NewAddress(c.ContainerID(), c.ObjectID())
	res := make([]objectcore.AddressWithAttributes, 0, len(withoutDel))
	for _, obj := range withoutDel {
		if obj.Address.Compare(cursorAddr) > 0 {
			res = append(res, obj)
		}
	}

	if len(res) == 0 {
		return nil, nil, engine.ErrEndOfListing
	}
	return res, engine.NewCursor(res[len(res)-1].Address.Container(), res[len(res)-1].Address.Object()), nil
}

func (x *testLocalNode) deletedObjects() []oid.Address {
	x.delMtx.RLock()
	res := slices.Collect(maps.Keys(x.del))
	x.delMtx.RUnlock()
	return res
}

func (x *testLocalNode) deletedShardCopies(addr oid.Address) []string {
	x.delMtx.RLock()
	res := slices.Clone(x.delByShard[addr])
	x.delMtx.RUnlock()
	return res
}

func (x *testLocalNode) Delete(addr oid.Address) error {
	x.delMtx.Lock()
	x.del[addr] = struct{}{}
	x.delMtx.Unlock()
	return nil
}

func (x *testLocalNode) Put(*object.Object, []byte) error {
	return nil
}

func (x *testLocalNode) Head(oid.Address, bool) (*object.Object, error) {
	return &object.Object{}, nil
}

func (x *testLocalNode) HeadECPart(cid.ID, oid.ID, iec.PartInfo) (object.Object, error) {
	return object.Object{}, nil
}

func (x *testLocalNode) GetRange(oid.Address, uint64, uint64) ([]byte, error) {
	panic("unimplemented")
}

func (x *testLocalNode) DeleteRedundantCopies(addr oid.Address, shardIDs []string) error {
	if x.deleteRedundantCopies != nil {
		return x.deleteRedundantCopies(addr, shardIDs)
	}

	if len(shardIDs) < 2 {
		return nil
	}

	x.delMtx.Lock()
	x.delByShard[addr] = append(x.delByShard[addr], shardIDs[1:]...)
	x.delMtx.Unlock()
	return nil
}

type getNodesKey struct {
	cnr cid.ID
	obj oid.ID
}

type getNodesValue struct {
	nodes    []netmap.NodeInfo
	repRules []uint
	ecRules  []iec.Rule
}

func newGetNodesKey(cnr cid.ID, obj oid.ID) getNodesKey {
	return getNodesKey{
		cnr: cnr,
		obj: obj,
	}
}

func (x *mockNetwork) setObjectNodesRepResult(cnr cid.ID, obj oid.ID, nodes []netmap.NodeInfo, rep uint) {
	x.mtx.Lock()
	x.getNodes[newGetNodesKey(cnr, obj)] = getNodesValue{
		nodes:    nodes,
		repRules: []uint{rep},
	}
	x.mtx.Unlock()
}

func (x *mockNetwork) setObjectNodesECResult(cnr cid.ID, obj oid.ID, nodes []netmap.NodeInfo, rule iec.Rule) {
	x.mtx.Lock()
	x.getNodes[newGetNodesKey(cnr, obj)] = getNodesValue{
		nodes:   nodes,
		ecRules: []iec.Rule{rule},
	}
	x.mtx.Unlock()
}

func (x *mockNetwork) GetNodesForObject(addr oid.Address) ([][]netmap.NodeInfo, []uint, []iec.Rule, error) {
	key := newGetNodesKey(addr.Container(), addr.Object())
	x.mtx.Lock()
	x.getNodesCalled[key]++
	v, ok := x.getNodes[key]
	x.mtx.Unlock()
	if !ok {
		return nil, nil, nil, errors.New("[test] unexpected policy requested")
	}

	return [][]netmap.NodeInfo{v.nodes}, v.repRules, v.ecRules, nil
}

func (x *mockNetwork) totalGetNodesCalls() uint64 {
	x.mtx.RLock()
	defer x.mtx.RUnlock()

	var res uint64
	for _, v := range x.getNodesCalled {
		res += v
	}
	return res
}

type connObjectKey struct {
	pubKey string
	obj    oid.Address
}

func newConnKey(node netmap.NodeInfo, objAddr oid.Address) connObjectKey {
	return connObjectKey{
		pubKey: string(node.PublicKey()),
		obj:    objAddr,
	}
}

type mockAPIConnections struct {
	lock sync.RWMutex
	head map[connObjectKey]error
}

func (x *mockAPIConnections) setHeadResult(node netmap.NodeInfo, addr oid.Address, err error) {
	x.lock.Lock()
	defer x.lock.Unlock()
	x.head[newConnKey(node, addr)] = err
}

func (x *mockAPIConnections) headObject(_ context.Context, node netmap.NodeInfo, addr oid.Address, _ bool, _ []string) (object.Object, error) {
	x.lock.RLock()
	defer x.lock.RUnlock()
	v, ok := x.head[newConnKey(node, addr)]
	if !ok {
		return object.Object{}, errors.New("[test] unexpected conn/object accessed")
	}
	return object.Object{}, v
}

func (x *mockAPIConnections) GetRange(context.Context, netmap.NodeInfo, cid.ID, oid.ID, uint64, uint64, []string) (io.ReadCloser, error) {
	panic("unimplemented")
}

func newMockAPIConnections() *mockAPIConnections {
	return &mockAPIConnections{
		head: make(map[connObjectKey]error),
	}
}
