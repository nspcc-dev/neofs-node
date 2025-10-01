package policer

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strconv"
	"sync"
	"testing"
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
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

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
					testRepCheck(t, defaultRep, localObj, nodes, i, true, allNotFound, expShortage, false, slices.Delete(slices.Clone(nodes), i, i+1))
				}
			})
		})
		t.Run("out container", func(t *testing.T) {
			expShortage := uint32(defaultRep)
			if broadcast {
				expShortage = uint32(len(nodes))
			}
			t.Run("in netmap", func(t *testing.T) {
				logBuf := testRepCheck(t, defaultRep, localObj, nodes, -1, true, allNotFound, expShortage, false, nodes)
				logBuf.AssertContains(testutil.LogEntry{
					Level: zap.InfoLevel, Message: "node outside the container, but nobody stores the object, holding the replica...", Fields: map[string]any{
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
}

func testRepCheck(t *testing.T, rep uint, localObj objectcore.AddressWithAttributes, nodes []netmap.NodeInfo, localIdx int, localInNM bool, headErrs []error,
	expShortage uint32, expRedundant bool, expCandidates []netmap.NodeInfo) *testutil.LogBuffer {
	require.Equal(t, len(nodes), len(headErrs))

	wp, err := ants.NewPool(100)
	require.NoError(t, err)

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

	r := newTestReplicator(t)

	conns := newMockAPIConnections()
	for i := range nodes {
		if i != localIdx {
			conns.setHeadResult(nodes[i], localObj.Address, headErrs[i])
		}
	}

	l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)
	p := New(
		WithPool(wp),
		WithReplicationCooldown(time.Hour), // any huge time to cancel process repeat
		WithNodeLoader(nopNodeLoader{}),
		WithNetwork(mockNet),
		WithLogger(l),
	)
	p.localStorage = localNode
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
		exp.SetObjectAddress(localObj.Address)
		exp.SetCopiesNumber(expShortage)
		exp.SetNodes(expCandidates)
		require.Equal(t, exp, r.task)
	} else {
		require.Never(t, repTaskSubmitted, 3*time.Second, 30*time.Millisecond)
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

				logBuf := testECCheck(t, rule, localObj, nodes, 0, allOK, false, nil, false)
				logBuf.AssertContains(testutil.LogEntry{
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

		logBuf := testECCheckWithNetwork(t, mockNet, localObj, nodes, 0, allOK, false, nil, false)
		logBuf.AssertContains(testutil.LogEntry{
			Level: zap.ErrorLevel, Message: "received EC parent OID with unexpected len from local storage, skip object",
			Fields: map[string]any{"component": "Object Policer", "object": localObj.Address.String(), "len": json.Number("31")},
		})
	})

	t.Run("EC part in non-EC container", func(t *testing.T) {
		mockNet := newMockNetwork()
		mockNet.setObjectNodesRepResult(localObj.Address.Container(), parentOID, nodes, 3)

		logBuf := testECCheckWithNetwork(t, mockNet, localObj, nodes, 0, allOK, true, nil, false)
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

			logBuf := testECCheck(t, rule, localObj, nodes, 0, allOK, true, nil, false)
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.InfoLevel, Message: "object with lacking EC attributes detected, deleting",
				Fields: map[string]any{"component": "Object Policer", "object": localObj.Address.String()},
			})
		})
		t.Run("too big rule index", func(t *testing.T) {
			localObj := localObj
			localObj.Attributes = slices.Clone(localObj.Attributes)
			localObj.Attributes[0] = "1"

			logBuf := testECCheck(t, rule, localObj, nodes, 0, allOK, true, nil, false)
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

			logBuf := testECCheck(t, rule, localObj, nodes, 0, allOK, true, nil, false)
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.WarnLevel, Message: "local object with invalid EC part index detected, deleting",
				Fields: map[string]any{"component": "Object Policer", "object": localObj.Address.String(), "rule": "17/4",
					"partIdx": json.Number("21")},
			})
		})
	})

	t.Run("local node is optimal", func(t *testing.T) {
		logBuf := testECCheck(t, rule, localObj, nodes, 5, all404, false, nil, false)
		logBuf.AssertContains(testutil.LogEntry{
			Level: zap.DebugLevel, Message: "local node is optimal for EC part, hold",
			Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
				"rule": "6/3", "partIdx": json.Number("5")},
		})
	})

	t.Run("found on more optimal node", func(t *testing.T) {
		errs := slices.Clone(all404)
		errs[5] = nil

		logBuf := testECCheck(t, rule, localObj, nodes, 6, errs, true, nil, false)
		logBuf.AssertContains(testutil.LogEntry{
			Level: zap.InfoLevel, Message: "EC part header successfully received from more optimal node, drop",
			Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
				"rule": "6/3", "partIdx": json.Number("5"), "node": []any{"localhost:10010", "localhost:10011"}},
		})
	})

	t.Run("not found on more optimal node", func(t *testing.T) {
		candidates := islices.CollectIndex(nodes, optimalOrder[:len(optimalOrder)-1]...)

		t.Run("move failure", func(t *testing.T) {
			logBuf := testECCheck(t, rule, localObj, nodes, optimalOrder[len(optimalOrder)-1], all404, false, candidates, false)
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

		logBuf := testECCheck(t, rule, localObj, nodes, optimalOrder[len(optimalOrder)-1], all404, true, candidates, true)
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

			logBuf := testECCheck(t, rule, localObj, nodes, optimalOrder[len(optimalOrder)-1], errs, true, nil, false)
			logBuf.AssertContains(testutil.LogEntry{
				Level: zap.InfoLevel, Message: "failed to receive EC part header from more optimal node due to its maintenance, continue",
				Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
					"rule": "6/3", "partIdx": json.Number("5"), "node": []any{"localhost:10008", "localhost:10009"}},
			})
		})

		logBuf := testECCheck(t, rule, localObj, nodes, optimalOrder[len(optimalOrder)-1], errs, false, nil, false)
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

				logBuf := testECCheck(t, rule, localObj, nodes, localIdx, errs, true, nil, false)
				checkErrsLog(logBuf)
				logBuf.AssertContains(testutil.LogEntry{
					Level: zap.InfoLevel, Message: "EC part header successfully received from more optimal node, drop",
					Fields: map[string]any{"component": "Object Policer", "cid": cnr.String(), "partOID": partOID.String(),
						"rule": "6/3", "partIdx": json.Number("5"), "node": holderField},
				})
			})

			t.Run("move failure", func(t *testing.T) {
				logBuf := testECCheck(t, rule, localObj, nodes, localIdx, errs, false, candidates, false)
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

			logBuf := testECCheck(t, rule, localObj, nodes, localIdx, errs, true, candidates, true)
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

func testECCheck(t *testing.T, rule iec.Rule, localObj objectcore.AddressWithAttributes, nodes []netmap.NodeInfo, localIdx int, headErrs []error, expRedundant bool, expCandidates []netmap.NodeInfo, repSuccess bool) *testutil.LogBuffer {
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

	return testECCheckWithNetwork(t, mockNet, localObj, nodes, localIdx, headErrs, expRedundant, expCandidates, repSuccess)
}

func testECCheckWithNetwork(t *testing.T, mockNet *mockNetwork, localObj objectcore.AddressWithAttributes, nodes []netmap.NodeInfo,
	localIdx int, headErrs []error, expRedundant bool, expCandidates []netmap.NodeInfo, repSuccess bool) *testutil.LogBuffer {
	expShortage := uint32(0)
	if len(expCandidates) > 0 {
		expShortage = 1
	}

	return testECCheckWithNetworkAndShortage(t, mockNet, localObj, nodes, localIdx, headErrs, expRedundant, expCandidates, expRedundant, expShortage)
}

func testECCheckWithNetworkAndShortage(t *testing.T, mockNet *mockNetwork, localObj objectcore.AddressWithAttributes, nodes []netmap.NodeInfo,
	localIdx int, headErrs []error, expRedundant bool, expCandidates []netmap.NodeInfo, repSuccess bool, expShortage uint32) *testutil.LogBuffer {
	require.Equal(t, len(nodes), len(headErrs))

	wp, err := ants.NewPool(100)
	require.NoError(t, err)

	localNode := newTestLocalNode()
	localNode.objList = []objectcore.AddressWithAttributes{localObj}

	r := newTestReplicator(t)
	r.success = repSuccess

	conns := newMockAPIConnections()
	for i := range nodes {
		if i != localIdx {
			conns.setHeadResult(nodes[i], localObj.Address, headErrs[i])
		}
	}

	l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)
	p := New(
		WithPool(wp),
		WithReplicationCooldown(time.Hour), // any huge time to cancel process repeat
		WithNodeLoader(nopNodeLoader{}),
		WithNetwork(mockNet),
		WithLogger(l),
	)
	p.localStorage = localNode
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
	if len(expCandidates) > 0 {
		require.Eventually(t, repTaskSubmitted, 3*time.Second, 30*time.Millisecond)

		var exp replicator.Task
		exp.SetObjectAddress(localObj.Address)
		exp.SetCopiesNumber(expShortage)
		exp.SetNodes(expCandidates)
		require.Equal(t, exp, r.task)
	} else {
		require.Never(t, repTaskSubmitted, 3*time.Second, 30*time.Millisecond)
	}

	if expRedundant {
		require.Equal(t, []oid.Address{localObj.Address}, localNode.deletedObjects())
	} else {
		require.Empty(t, localNode.deletedObjects())
	}

	return lb
}

type testReplicator struct {
	t         *testing.T
	task      replicator.Task
	gotTaskCh chan struct{}
	success   bool
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

	nodes := task.Nodes()
	require.NotEmpty(x.t, nodes)
	if x.success {
		r.SubmitSuccessfulReplication(nodes[0])
	}

	x.task = task
	close(x.gotTaskCh)
}

type testLocalNode struct {
	objList []objectcore.AddressWithAttributes

	delMtx sync.RWMutex
	del    map[oid.Address]struct{}
}

func newTestLocalNode() *testLocalNode {
	return &testLocalNode{
		del: make(map[oid.Address]struct{}),
	}
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

func (x *testLocalNode) ListWithCursor(uint32, *engine.Cursor, ...string) ([]objectcore.AddressWithAttributes, *engine.Cursor, error) {
	return x.objList, nil, nil
}

func (x *testLocalNode) deletedObjects() []oid.Address {
	x.delMtx.RLock()
	res := slices.Collect(maps.Keys(x.del))
	x.delMtx.RUnlock()
	return res
}

func (x *testLocalNode) Delete(addr oid.Address) error {
	x.delMtx.Lock()
	x.del[addr] = struct{}{}
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
	x.getNodes[newGetNodesKey(cnr, obj)] = getNodesValue{
		nodes:    nodes,
		repRules: []uint{rep},
	}
}

func (x *mockNetwork) setObjectNodesECResult(cnr cid.ID, obj oid.ID, nodes []netmap.NodeInfo, rule iec.Rule) {
	x.getNodes[newGetNodesKey(cnr, obj)] = getNodesValue{
		nodes:   nodes,
		ecRules: []iec.Rule{rule},
	}
}

func (x *mockNetwork) GetNodesForObject(addr oid.Address) ([][]netmap.NodeInfo, []uint, []iec.Rule, error) {
	v, ok := x.getNodes[newGetNodesKey(addr.Container(), addr.Object())]
	if !ok {
		return nil, nil, nil, errors.New("[test] unexpected policy requested")
	}

	return [][]netmap.NodeInfo{v.nodes}, v.repRules, v.ecRules, nil
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
