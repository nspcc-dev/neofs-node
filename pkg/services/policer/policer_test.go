package policer

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPolicer_Run_RepDefault(t *testing.T) {
	const defaultRep = 3
	const defaultCBF = 3
	nodes := testutil.Nodes(defaultRep * defaultCBF)

	t.Run("all OK", func(t *testing.T) {
		allOK := islices.RepeatElement(len(nodes), error(nil))
		t.Run("in container", func(t *testing.T) {
			t.Run("primary", func(t *testing.T) {
				for i := range defaultRep {
					testRepCheck(t, defaultRep, nodes, i, true, allOK, 0, false, nil)
				}
			})
			t.Run("backup", func(t *testing.T) {
				for i := defaultRep; i < len(nodes); i++ {
					res := testRepCheck(t, defaultRep, nodes, i, true, allOK, 0, true, nil)
					res.logBuf.AssertContains(testutil.LogEntry{
						Level: zap.InfoLevel, Message: "local replica of the object is redundant in the container, removing...", Fields: map[string]any{
							"component": "Object Policer",
							"object":    oid.NewAddress(res.cnr, res.obj).String(),
						},
					})
				}
			})
		})
		t.Run("out container", func(t *testing.T) {
			t.Run("in netmap", func(t *testing.T) {
				res := testRepCheck(t, defaultRep, nodes, -1, true, allOK, 0, true, nil)
				res.logBuf.AssertContains(testutil.LogEntry{
					Level: zap.InfoLevel, Message: "node outside the container, removing the replica so as not to violate the storage policy...", Fields: map[string]any{
						"component": "Object Policer",
						"object":    oid.NewAddress(res.cnr, res.obj).String(),
					},
				})
			})
			t.Run("out netmap", func(t *testing.T) {
				res := testRepCheck(t, defaultRep, nodes, -1, false, allOK, 0, false, nil)
				res.logBuf.AssertContains(testutil.LogEntry{
					Level: zap.InfoLevel, Message: "node is outside the network map, holding the replica...", Fields: map[string]any{
						"component": "Object Policer",
						"object":    oid.NewAddress(res.cnr, res.obj).String(),
					},
				})
			})
		})
	})

	t.Run("all 404", func(t *testing.T) {
		allNotFound := islices.RepeatElement(len(nodes), error(apistatus.ErrObjectNotFound))
		t.Run("in container", func(t *testing.T) {
			t.Run("primary", func(t *testing.T) {
				for i := range defaultRep {
					testRepCheck(t, defaultRep, nodes, i, true, allNotFound, defaultRep-1, false, slices.Delete(slices.Clone(nodes), i, i+1))
				}
			})
			t.Run("backup", func(t *testing.T) {
				for i := defaultRep; i < len(nodes); i++ {
					testRepCheck(t, defaultRep, nodes, i, true, allNotFound, defaultRep-1, false, slices.Delete(slices.Clone(nodes), i, i+1))
				}
			})
		})
		t.Run("out container", func(t *testing.T) {
			t.Run("in netmap", func(t *testing.T) {
				res := testRepCheck(t, defaultRep, nodes, -1, true, allNotFound, defaultRep, false, nodes)
				res.logBuf.AssertContains(testutil.LogEntry{
					Level: zap.InfoLevel, Message: "node outside the container, but nobody stores the object, holding the replica...", Fields: map[string]any{
						"component": "Object Policer",
						"object":    oid.NewAddress(res.cnr, res.obj).String(),
					},
				})
			})
			t.Run("out netmap", func(t *testing.T) {
				testRepCheck(t, defaultRep, nodes, -1, false, allNotFound, defaultRep, false, nodes)
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
				for i := range defaultRep {
					res := testRepCheck(t, defaultRep, nodes, i, true, errs, 0, false, nil)
					res.logBuf.AssertContains(testutil.LogEntry{
						Level: zap.DebugLevel, Message: "some of the copies are stored on nodes under maintenance, save local copy", Fields: map[string]any{
							"component": "Object Policer",
							"count":     json.Number("2"),
						},
					})
					for j := range defaultRep {
						if j == i {
							continue
						}
						res.logBuf.AssertContains(testutil.LogEntry{
							Level: zap.DebugLevel, Message: "consider node under maintenance as OK", Fields: map[string]any{
								"component": "Object Policer",
								"node":      hex.EncodeToString(nodes[j].PublicKey()),
							},
						})
					}
				}
			})
			t.Run("backup", func(t *testing.T) {
				for i := defaultRep; i < len(nodes); i++ {
					res := testRepCheck(t, defaultRep, nodes, i, true, errs, 0, false, nil)
					res.logBuf.AssertContains(testutil.LogEntry{
						Level: zap.DebugLevel, Message: "some of the copies are stored on nodes under maintenance, save local copy", Fields: map[string]any{
							"component": "Object Policer",
							"count":     json.Number("3"),
						},
					})
					for j := range defaultRep {
						res.logBuf.AssertContains(testutil.LogEntry{
							Level: zap.DebugLevel, Message: "consider node under maintenance as OK", Fields: map[string]any{
								"component": "Object Policer",
								"node":      hex.EncodeToString(nodes[j].PublicKey()),
							},
						})
					}
				}
			})
		})
		t.Run("out container", func(t *testing.T) {
			t.Run("in netmap", func(t *testing.T) {
				res := testRepCheck(t, defaultRep, nodes, -1, true, errs, 0, false, nil)
				res.logBuf.AssertContains(testutil.LogEntry{
					Level: zap.DebugLevel, Message: "some of the copies are stored on nodes under maintenance, save local copy", Fields: map[string]any{
						"component": "Object Policer",
						"count":     json.Number("3"),
					},
				})
				for j := range defaultRep {
					res.logBuf.AssertContains(testutil.LogEntry{
						Level: zap.DebugLevel, Message: "consider node under maintenance as OK", Fields: map[string]any{
							"component": "Object Policer",
							"node":      hex.EncodeToString(nodes[j].PublicKey()),
						},
					})
				}
			})
			t.Run("out netmap", func(t *testing.T) {
				res := testRepCheck(t, defaultRep, nodes, -1, false, errs, 0, false, nil)
				res.logBuf.AssertContains(testutil.LogEntry{
					Level: zap.DebugLevel, Message: "some of the copies are stored on nodes under maintenance, save local copy", Fields: map[string]any{
						"component": "Object Policer",
						"count":     json.Number("3"),
					},
				})
				for j := range defaultRep {
					res.logBuf.AssertContains(testutil.LogEntry{
						Level: zap.DebugLevel, Message: "consider node under maintenance as OK", Fields: map[string]any{
							"component": "Object Policer",
							"node":      hex.EncodeToString(nodes[j].PublicKey()),
						},
					})
				}
			})
		})
	})

	t.Run("various errors", func(t *testing.T) {
		t.Run("in container", func(t *testing.T) {
			t.Run("primary", func(t *testing.T) {
				errs := islices.RepeatElement(len(nodes), error(apistatus.ErrObjectNotFound))
				errs[0] = errors.New("non-status error")
				errs[1] = nil

				res := testRepCheck(t, defaultRep, nodes, 2, true, errs, 1, false, slices.Clone(nodes)[3:])
				e := testutil.LogEntry{
					Level:   zap.ErrorLevel,
					Message: "receive object header to check policy compliance",
					Fields: map[string]any{
						"component": "Object Policer",
						"object":    oid.NewAddress(res.cnr, res.obj).String(),
						"error":     "non-status error",
					},
				}
				res.logBuf.AssertContains(e)

				errs[1] = apistatus.ErrServerInternal

				res = testRepCheck(t, defaultRep, nodes, defaultRep-1, true, errs, 2, false, slices.Clone(nodes)[defaultRep:])
				e.Fields["object"] = oid.NewAddress(res.cnr, res.obj).String()
				res.logBuf.AssertContains(e)
				e.Fields["error"] = errs[1].Error()
				res.logBuf.AssertContains(e)
			})
			t.Run("backup", func(t *testing.T) {
				errs := islices.RepeatElement(len(nodes), error(apistatus.ErrObjectNotFound))
				errs[0] = errors.New("non-status error")
				errs[1] = nil
				errs[2] = nil

				res := testRepCheck(t, defaultRep, nodes, defaultRep, true, errs, 0, false, slices.Clone(nodes)[defaultRep+1:])
				e := testutil.LogEntry{
					Level:   zap.ErrorLevel,
					Message: "receive object header to check policy compliance",
					Fields: map[string]any{
						"component": "Object Policer",
						"object":    oid.NewAddress(res.cnr, res.obj).String(),
						"error":     "non-status error",
					},
				}
				res.logBuf.AssertContains(e)

				errs[2] = apistatus.ErrObjectAlreadyRemoved

				candidates := slices.Clone(nodes)[defaultRep:]
				candidates = slices.Delete(candidates, 1, 2)

				res = testRepCheck(t, defaultRep, nodes, defaultRep+1, true, errs, 1, false, candidates)
				e.Fields["object"] = oid.NewAddress(res.cnr, res.obj).String()
				res.logBuf.AssertContains(e)
				e.Fields["error"] = errs[2].Error()
				res.logBuf.AssertContains(e)
			})
		})
		t.Run("out container", func(t *testing.T) {
			t.Run("in netmap", func(t *testing.T) {
				errs := islices.RepeatElement(len(nodes), error(apistatus.ErrObjectNotFound))
				errs[0] = errors.New("non-status error")
				errs[1] = nil
				errs[2] = nil

				res := testRepCheck(t, defaultRep, nodes, -1, true, errs, 1, true, slices.Clone(nodes)[3:])
				e := testutil.LogEntry{
					Level:   zap.ErrorLevel,
					Message: "receive object header to check policy compliance",
					Fields: map[string]any{
						"component": "Object Policer",
						"object":    oid.NewAddress(res.cnr, res.obj).String(),
						"error":     "non-status error",
					},
				}
				res.logBuf.AssertContains(e)

				errs[2] = apistatus.ErrServerInternal

				res = testRepCheck(t, defaultRep, nodes, -1, true, errs, 2, true, slices.Clone(nodes)[defaultRep:])
				e.Fields["object"] = oid.NewAddress(res.cnr, res.obj).String()
				res.logBuf.AssertContains(e)
				e.Fields["error"] = errs[2].Error()
				res.logBuf.AssertContains(e)
			})
			t.Run("out netmap", func(t *testing.T) {
				errs := islices.RepeatElement(len(nodes), error(apistatus.ErrObjectNotFound))
				errs[0] = errors.New("non-status error")
				errs[1] = nil
				errs[2] = nil

				res := testRepCheck(t, defaultRep, nodes, -1, false, errs, 1, false, slices.Clone(nodes)[3:])
				e := testutil.LogEntry{
					Level:   zap.ErrorLevel,
					Message: "receive object header to check policy compliance",
					Fields: map[string]any{
						"component": "Object Policer",
						"object":    oid.NewAddress(res.cnr, res.obj).String(),
						"error":     "non-status error",
					},
				}
				res.logBuf.AssertContains(e)

				errs[2] = apistatus.ErrServerInternal

				res = testRepCheck(t, defaultRep, nodes, -1, false, errs, 2, false, slices.Clone(nodes)[defaultRep:])
				e.Fields["object"] = oid.NewAddress(res.cnr, res.obj).String()
				res.logBuf.AssertContains(e)
				e.Fields["error"] = errs[2].Error()
				res.logBuf.AssertContains(e)
			})
		})
	})
}

type testRepCheckResult struct {
	cnr    cid.ID
	obj    oid.ID
	logBuf *testutil.LogBuffer
}

func testRepCheck(t *testing.T, rep uint, nodes []netmap.NodeInfo, localIdx int, localInNM bool, headErrs []error,
	expShortage uint32, expRedundant bool, expCandidates []netmap.NodeInfo) testRepCheckResult {
	require.Equal(t, len(nodes), len(headErrs))

	wp, err := ants.NewPool(100)
	require.NoError(t, err)

	cnr := cidtest.ID()
	objID := oidtest.ID()
	objAddr := oid.NewAddress(cnr, objID)

	localNode := testLocalNode{
		objList: []objectcore.AddressWithType{
			{Address: objAddr, Type: object.TypeRegular},
		},
	}

	mockNet := newMockNetwork()
	mockNet.setObjectNodesResult(cnr, objID, slices.Clone(nodes), rep)
	if localIdx >= 0 {
		mockNet.pubKey = nodes[localIdx].PublicKey()
		mockNet.inNetmap = true
	} else {
		mockNet.inNetmap = localInNM
	}

	r := newTestReplicator(t)

	conns := newMockAPIConnections()
	for i := range nodes {
		if i != localIdx {
			conns.setHeadResult(nodes[i], objAddr, headErrs[i])
		}
	}

	var redundantAddr atomic.Value // oid.Address
	l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)
	p := New(
		WithPool(wp),
		WithReplicationCooldown(time.Hour), // any huge time to cancel process repeat
		WithNodeLoader(nopNodeLoader{}),
		WithNetwork(mockNet),
		WithRedundantCopyCallback(func(addr oid.Address) { redundantAddr.Store(addr) }),
		WithLogger(l),
	)
	p.localStorage = &localNode
	p.apiConns = conns
	p.replicator = r

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Run(ctx)

	repTaskSubmitted := func() bool {
		select {
		case <-r.gotTaskCh:
			return true
		default:
			return false
		}
	}
	if expShortage > 0 {
		require.Eventually(t, repTaskSubmitted, 3*time.Second, 30*time.Millisecond)

		var exp replicator.Task
		exp.SetObjectAddress(objAddr)
		exp.SetCopiesNumber(expShortage)
		exp.SetNodes(expCandidates)
		require.Equal(t, exp, r.task)
	} else {
		require.Never(t, repTaskSubmitted, 3*time.Second, 30*time.Millisecond)
	}

	if expRedundant {
		require.Equal(t, objAddr, redundantAddr.Load())
	} else {
		require.Nil(t, redundantAddr.Load())
	}

	return testRepCheckResult{
		cnr:    cnr,
		obj:    objID,
		logBuf: lb,
	}
}

type testReplicator struct {
	t         *testing.T
	task      replicator.Task
	gotTaskCh chan struct{}
}

func newTestReplicator(t *testing.T) *testReplicator {
	return &testReplicator{
		t:         t,
		gotTaskCh: make(chan struct{}),
	}
}

func (x *testReplicator) HandleTask(ctx context.Context, task replicator.Task, r replicator.TaskResult) {
	require.NotNil(x.t, ctx)
	require.NotNil(x.t, r)

	x.task = task
	close(x.gotTaskCh)
}

type testLocalNode struct {
	objList []objectcore.AddressWithType
}

type mockNetwork struct {
	pubKey []byte

	inNetmap bool

	getNodes map[getNodesKey]getNodesValue
}

func newMockNetwork() *mockNetwork {
	return &mockNetwork{
		getNodes: make(map[getNodesKey]getNodesValue),
	}
}

func (x *mockNetwork) IsLocalNodePublicKey(key []byte) bool {
	return bytes.Equal(key, x.pubKey)
}

func (x *mockNetwork) IsLocalNodeInNetmap() bool {
	return x.inNetmap
}

func (x *testLocalNode) ListWithCursor(uint32, *engine.Cursor) ([]objectcore.AddressWithType, *engine.Cursor, error) {
	return x.objList, nil, nil
}

func (x *testLocalNode) Delete(oid.Address) error {
	panic("unimplemented")
}

type getNodesKey struct {
	cnr cid.ID
	obj oid.ID
}

type getNodesValue struct {
	nodes []netmap.NodeInfo
	rep   uint
}

func newGetNodesKey(cnr cid.ID, obj oid.ID) getNodesKey {
	return getNodesKey{
		cnr: cnr,
		obj: obj,
	}
}

func (x *mockNetwork) setObjectNodesResult(cnr cid.ID, obj oid.ID, nodes []netmap.NodeInfo, rep uint) {
	x.getNodes[newGetNodesKey(cnr, obj)] = getNodesValue{
		nodes: nodes,
		rep:   rep,
	}
}

func (x *mockNetwork) GetNodesForObject(addr oid.Address) ([][]netmap.NodeInfo, []uint, error) {
	v, ok := x.getNodes[newGetNodesKey(addr.Container(), addr.Object())]
	if !ok {
		return nil, nil, errors.New("[test] unexpected policy requested")
	}

	return [][]netmap.NodeInfo{v.nodes}, []uint{v.rep}, nil
}

type nopNodeLoader struct{}

func (nopNodeLoader) ObjectServiceLoad() float64 {
	return 0
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
	head map[connObjectKey]error
}

func (x *mockAPIConnections) setHeadResult(node netmap.NodeInfo, addr oid.Address, err error) {
	x.head[newConnKey(node, addr)] = err
}

func (x *mockAPIConnections) headObject(_ context.Context, node netmap.NodeInfo, addr oid.Address) (object.Object, error) {
	v, ok := x.head[newConnKey(node, addr)]
	if !ok {
		return object.Object{}, errors.New("[test] unexpected conn/object accessed")
	}
	return object.Object{}, v
}

func newMockAPIConnections() *mockAPIConnections {
	return &mockAPIConnections{
		head: make(map[connObjectKey]error),
	}
}
