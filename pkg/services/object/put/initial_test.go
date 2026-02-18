package putsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type nopObjectWriter struct{}

func (nopObjectWriter) Write(p []byte) (int, error)    { return len(p), nil }
func (nopObjectWriter) Close() error                   { return nil }
func (nopObjectWriter) GetResult() client.ResObjectPut { return client.ResObjectPut{} }

type nopPutClient struct {
	unimplementedClient
}

func (nopPutClient) ObjectPutInit(context.Context, object.Object, user.Signer, client.PrmObjectPutInit) (client.ObjectWriter, error) {
	return nopObjectWriter{}, nil
}

type testConnCollector struct {
	errors map[string]error

	connsMtx sync.Mutex
	conns    [][]byte
}

func (x *testConnCollector) Get(node coreclient.NodeInfo) (coreclient.MultiAddressClient, error) {
	x.connsMtx.Lock()
	x.conns = append(x.conns, node.PublicKey())
	x.connsMtx.Unlock()

	if err := x.errors[string(node.PublicKey())]; err != nil {
		return nil, err
	}

	return nopPutClient{}, nil
}

func (x *testConnCollector) failNode(node netmap.NodeInfo) {
	if x.errors == nil {
		x.errors = map[string]error{}
	}
	x.errors[string(node.PublicKey())] = errors.New("[test] forced error")
}

func (x *testConnCollector) collectConns() [][]byte {
	return x.conns
}

type testLocalStorageCallCollector struct {
	unimplementedLocalStorage
	count atomic.Uint32
}

func (x *testLocalStorageCallCollector) Put(*object.Object, []byte) error {
	x.count.Add(1)
	return nil
}

func (x *testLocalStorageCallCollector) callCount() uint32 {
	return x.count.Load()
}

func TestInitialPlacement_REPLimits(t *testing.T) {
	anyNodeKey := neofscryptotest.Signer().ECDSAPrivateKey
	obj := objecttest.Object()
	var noEncObj encodedObject
	keyStorage := util.NewKeyStorage(&anyNodeKey, nil, nil)

	t.Run("failures", func(t *testing.T) {
		tgt := &distributedTarget{
			placementIterator: placementIterator{
				log:      zap.NewNop(),
				neoFSNet: testNetwork{},
			},
			keyStorage: keyStorage,
		}

		for i, tc := range []struct {
			main       []uint
			initial    []uint32
			failNodes  [][]int
			okNodes    int
			failedRule int
			lastNode   int
		}{
			{main: []uint{3}, initial: []uint32{1}, failNodes: [][]int{
				{0, 1, 2},
			}, okNodes: 0, failedRule: 0, lastNode: 2},
			{main: []uint{3}, initial: []uint32{2}, failNodes: [][]int{
				{0, 1},
			}, okNodes: 0, failedRule: 0, lastNode: 1},
			{main: []uint{3, 3, 3}, initial: []uint32{1, 1, 1}, failNodes: [][]int{
				{0, 1}, {0, 1, 2}, {0, 1},
			}, okNodes: 0, failedRule: 1, lastNode: 2},
			{main: []uint{1, 7, 11}, initial: []uint32{1, 3, 6}, failNodes: [][]int{
				{}, {1, 3}, {0, 2, 4, 6, 8, 10},
			}, okNodes: 5, failedRule: 2, lastNode: 10},
			{main: []uint{1, 7, 11}, initial: []uint32{1, 3, 6}, failNodes: [][]int{
				{}, {0, 1, 2, 4, 5}, {},
			}, okNodes: 1, failedRule: 1, lastNode: 5},
			{main: []uint{1, 7, 11}, initial: []uint32{1, 3, 6}, failNodes: [][]int{
				{}, {0, 1, 2, 3, 4}, {}, // 0, 1, 2, 3, 4, 5, 6
			}, okNodes: 1, failedRule: 1, lastNode: 5},
		} {
			nodeLists := allocNodes(tc.main)
			inPolicy := InitialPlacementPolicy{
				MaxPerRuleReplicas: tc.initial,
			}

			var clientConns testConnCollector
			for listIdx := range tc.failNodes {
				for _, nodeIdx := range tc.failNodes[listIdx] {
					clientConns.failNode(nodeLists[listIdx][nodeIdx])
				}
			}

			tgt.clientConstructor = &clientConns

			err := tgt.handleInitialPlacementPolicy(inPolicy, tc.main, nil, nodeLists, obj, noEncObj)
			require.Error(t, err, i)

			leftNodes := len(nodeLists[tc.failedRule]) - tc.okNodes - len(tc.failNodes[tc.failedRule])
			expErr := fmt.Sprintf("unable to reach replica number for rule #%d (required: %d, succeeded: %d, left nodes: %d)",
				tc.failedRule, tc.initial[tc.failedRule], tc.okNodes, leftNodes)
			require.ErrorContains(t, err, expErr, i)
			if tc.okNodes > 0 {
				require.ErrorIs(t, err, apistatus.ErrIncomplete, i)
			}

			calledNodes := clientConns.collectConns()
			for nodeIdx, node := range nodeLists[tc.failedRule] {
				if nodeIdx <= tc.lastNode {
					require.Contains(t, calledNodes, node.PublicKey())
				} else {
					require.NotContains(t, calledNodes, node.PublicKey())
				}
			}
		}
	})

	t.Run("fault tolerance", func(t *testing.T) {
		tgt := &distributedTarget{
			placementIterator: placementIterator{
				neoFSNet: testNetwork{},
			},
			keyStorage: keyStorage,
		}

		for i, tc := range []struct {
			main      []uint
			initial   []uint32
			failNodes [][]int
			okNodes   [][]int
		}{
			{main: []uint{3, 3, 3}, initial: []uint32{1, 1, 1}, failNodes: [][]int{
				{0, 1}, {0, 1}, {0, 1},
			}, okNodes: [][]int{
				{2}, {2}, {2},
			}},
			{main: []uint{1, 7, 11}, initial: []uint32{1, 3, 5}, failNodes: [][]int{
				{}, {1, 3}, {0, 2, 4, 5},
			}, okNodes: [][]int{
				{0}, {0, 2, 4}, {1, 3, 6, 7, 8},
			}},
		} {
			nodeLists := allocNodes(tc.main)
			inPolicy := InitialPlacementPolicy{
				MaxPerRuleReplicas: tc.initial,
			}

			var clientConns testConnCollector
			for listIdx := range tc.failNodes {
				for _, nodeIdx := range tc.failNodes[listIdx] {
					clientConns.failNode(nodeLists[listIdx][nodeIdx])
				}
			}

			l, logBuf := testutil.NewBufferedLogger(t, zap.ErrorLevel)

			tgt.clientConstructor = &clientConns
			tgt.placementIterator.log = l

			err := tgt.handleInitialPlacementPolicy(inPolicy, tc.main, nil, nodeLists, obj, noEncObj)
			require.NoError(t, err, i)

			calledNodes := clientConns.collectConns()

			for listIdx := range nodeLists {
				for nodeIdx, node := range nodeLists[listIdx] {
					if slices.Contains(tc.failNodes[listIdx], nodeIdx) {
						logBuf.AssertContains(testutil.LogEntry{
							Level:   zap.ErrorLevel,
							Message: "initial REP placement to node failed",
							Fields: map[string]any{
								"object": obj.Address().String(), "repRule": json.Number(strconv.Itoa(listIdx)), "nodeIdx": json.Number(strconv.Itoa(nodeIdx)), "local": false,
								"error": fmt.Sprintf("could not close object stream: could not create SDK client %x: [test] forced error", node.PublicKey()),
							},
						})
					} else if !slices.Contains(tc.okNodes[listIdx], nodeIdx) {
						require.NotContains(t, calledNodes, node.PublicKey(), i)
						continue
					}

					require.Contains(t, calledNodes, node.PublicKey(), i)
				}
			}
		}
	})

	tgt := &distributedTarget{
		placementIterator: placementIterator{
			neoFSNet: testNetwork{},
		},
		keyStorage: keyStorage,
	}

	for i, tc := range []struct {
		main    []uint
		initial []uint32
	}{
		{main: []uint{3, 3, 3}, initial: []uint32{3, 2, 1}},
		{main: []uint{3, 3, 3}, initial: []uint32{1, 2, 3}},
		{main: []uint{3, 3, 3}, initial: []uint32{1, 1, 1}},
		{main: []uint{3, 3, 3}, initial: []uint32{1, 0, 1}},
		{main: []uint{3, 3, 3}, initial: []uint32{1, 0, 0}},
		{main: []uint{3, 3, 3}, initial: []uint32{0, 0, 1}},
		{main: []uint{1, 5, 10}, initial: []uint32{0, 3, 5}},
	} {
		nodeLists := allocNodes(tc.main)
		inPolicy := InitialPlacementPolicy{
			MaxPerRuleReplicas: tc.initial,
		}

		var clientConns testConnCollector
		tgt.clientConstructor = &clientConns

		err := tgt.handleInitialPlacementPolicy(inPolicy, tc.main, nil, nodeLists, obj, noEncObj)
		require.NoError(t, err, i)

		calledNodes := clientConns.collectConns()

		for listIdx := range nodeLists {
			for nodeIdx := range nodeLists[listIdx] {
				if nodeIdx < int(tc.initial[listIdx]) {
					require.Contains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
				} else {
					require.NotContains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
				}
			}
		}
	}
}

func TestInitialPlacement_MaxReplicas(t *testing.T) {
	anyNodeKey := neofscryptotest.Signer().ECDSAPrivateKey
	obj := objecttest.Object()
	var noEncObj encodedObject
	keyStorage := util.NewKeyStorage(&anyNodeKey, nil, nil)

	t.Run("failures", func(t *testing.T) {
		tgt := &distributedTarget{
			placementIterator: placementIterator{
				log:      zap.NewNop(),
				neoFSNet: testNetwork{},
			},
			keyStorage: keyStorage,
		}

		for i, tc := range []struct {
			main        []uint
			maxReplicas uint32
			failNodes   [][2]int
			called      [][2]int
		}{
			{main: []uint{3, 3, 3}, maxReplicas: 1, failNodes: [][2]int{
				{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2}, {1, 2}, {2, 2},
			}, called: [][2]int{
				{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2}, {1, 2}, {2, 2},
			}},
			{main: []uint{3, 3, 3}, maxReplicas: 6, failNodes: [][2]int{
				{0, 0}, {1, 1}, {1, 2}, {2, 2},
			}, called: [][2]int{
				{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2}, {1, 2}, {2, 2},
			}},
		} {
			nodeLists := allocNodes(tc.main)
			inPolicy := InitialPlacementPolicy{
				MaxReplicas: tc.maxReplicas,
			}

			var clientConns testConnCollector
			for _, failIdx := range tc.failNodes {
				clientConns.failNode(nodeLists[failIdx[0]][failIdx[1]])
			}

			tgt.clientConstructor = &clientConns

			err := tgt.handleInitialPlacementPolicy(inPolicy, tc.main, nil, nodeLists, obj, noEncObj)
			require.Error(t, err)

			leftNodes := islices.TwoDimSliceElementCount(nodeLists) - len(tc.called)
			expErr := fmt.Sprintf("unable to reach MaxReplicas %d (succeeded: %d, left nodes: %d)",
				tc.maxReplicas, len(tc.called)-len(tc.failNodes), leftNodes)
			require.ErrorContains(t, err, expErr, i)
			if len(tc.called)-len(tc.failNodes) > 0 {
				require.ErrorIs(t, err, apistatus.ErrIncomplete, i)
			}

			calledNodes := clientConns.collectConns()
			require.Len(t, calledNodes, len(tc.called))
			for listIdx := range nodeLists {
				for nodeIdx := range nodeLists[listIdx] {
					if slices.Contains(tc.called, [2]int{listIdx, nodeIdx}) {
						require.Contains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
					} else {
						require.NotContains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
					}
				}
			}
		}
	})

	t.Run("fault tolerance", func(t *testing.T) {
		tgt := &distributedTarget{
			placementIterator: placementIterator{
				log:      zap.NewNop(),
				neoFSNet: testNetwork{},
			},
			keyStorage: keyStorage,
		}

		for i, tc := range []struct {
			main        []uint
			maxReplicas uint32
			failNodes   [][2]int
			called      [][2]int
		}{
			{main: []uint{3, 3, 3}, maxReplicas: 1, failNodes: [][2]int{
				{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2}, {1, 2},
			}, called: [][2]int{
				{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2}, {1, 2}, {2, 2},
			}},
			{main: []uint{3, 3, 3}, maxReplicas: 6, failNodes: [][2]int{
				{0, 0}, {1, 1}, {1, 2},
			}, called: [][2]int{
				{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2}, {1, 2}, {2, 2},
			}},
		} {
			nodeLists := allocNodes(tc.main)
			inPolicy := InitialPlacementPolicy{
				MaxReplicas: tc.maxReplicas,
			}

			var clientConns testConnCollector
			for _, failIdx := range tc.failNodes {
				clientConns.failNode(nodeLists[failIdx[0]][failIdx[1]])
			}

			tgt.clientConstructor = &clientConns

			err := tgt.handleInitialPlacementPolicy(inPolicy, tc.main, nil, nodeLists, obj, noEncObj)
			require.NoError(t, err, i)

			calledNodes := clientConns.collectConns()
			require.Len(t, calledNodes, len(tc.called))
			for listIdx := range nodeLists {
				for nodeIdx := range nodeLists[listIdx] {
					if slices.Contains(tc.called, [2]int{listIdx, nodeIdx}) {
						require.Contains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
					} else {
						require.NotContains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
					}
				}
			}
		}
	})

	tgt := &distributedTarget{
		placementIterator: placementIterator{
			neoFSNet: testNetwork{},
		},
		keyStorage: keyStorage,
	}

	for i, tc := range []struct {
		main        []uint
		maxReplicas uint32
		called      [][2]int
	}{
		{main: []uint{3, 3, 3}, maxReplicas: 1, called: [][2]int{
			{0, 0},
		}},
		{main: []uint{3, 3, 3}, maxReplicas: 7, called: [][2]int{
			{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2},
		}},
		{main: []uint{3, 1, 3}, maxReplicas: 6, called: [][2]int{
			{0, 0}, {1, 0}, {2, 0}, {0, 1}, {2, 1}, {0, 2},
		}},
	} {
		nodeLists := allocNodes(tc.main)
		inPolicy := InitialPlacementPolicy{
			MaxReplicas: tc.maxReplicas,
		}

		var clientConns testConnCollector
		tgt.clientConstructor = &clientConns

		err := tgt.handleInitialPlacementPolicy(inPolicy, tc.main, nil, nodeLists, obj, noEncObj)
		require.NoError(t, err, i)

		calledNodes := clientConns.collectConns()
		require.Len(t, calledNodes, len(tc.called))
		for listIdx := range nodeLists {
			for nodeIdx := range nodeLists[listIdx] {
				if slices.Contains(tc.called, [2]int{listIdx, nodeIdx}) {
					require.Contains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
				} else {
					require.NotContains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
				}
			}
		}
	}
}

func TestInitialPlacement_MaxReplicas_PreferLocal(t *testing.T) {
	anyNodeSigner := neofscryptotest.Signer()
	anyNodeKey := anyNodeSigner.ECDSAPrivateKey
	obj := objecttest.Object()
	var noEncObj encodedObject
	keyStorage := util.NewKeyStorage(&anyNodeKey, nil, nil)
	localPubKey := anyNodeSigner.PublicKeyBytes

	mainRepRules := []uint{3, 3, 3}

	nodeLists := allocNodes(mainRepRules)
	nodeLists[1][1].SetPublicKey(localPubKey)

	inPolicy := InitialPlacementPolicy{
		MaxReplicas: 7,
		PreferLocal: true,
	}

	var clientConns testConnCollector
	clientConns.failNode(nodeLists[1][0])

	var localStorage testLocalStorageCallCollector

	tgt := &distributedTarget{
		placementIterator: placementIterator{
			log:      zap.NewNop(),
			neoFSNet: testNetwork{localPubKey: localPubKey},
		},
		keyStorage:        keyStorage,
		clientConstructor: &clientConns,
		localStorage:      &localStorage,
	}

	err := tgt.handleInitialPlacementPolicy(inPolicy, mainRepRules, nil, nodeLists, obj, noEncObj)
	require.NoError(t, err)

	calledNodes := clientConns.collectConns()
	require.EqualValues(t, 1, localStorage.callCount())
	require.Len(t, calledNodes, 7)
	require.Contains(t, calledNodes, nodeLists[1][0].PublicKey())
	require.Contains(t, calledNodes, nodeLists[1][2].PublicKey())
	require.Contains(t, calledNodes, nodeLists[0][0].PublicKey())
	require.Contains(t, calledNodes, nodeLists[2][0].PublicKey())
	require.Contains(t, calledNodes, nodeLists[0][1].PublicKey())
	require.Contains(t, calledNodes, nodeLists[2][1].PublicKey())
}

func TestInitialPlacement_EC(t *testing.T) {
	anyNodeSigner := neofscryptotest.Signer()
	anyNodeKey := anyNodeSigner.ECDSAPrivateKey
	obj := objecttest.Object()
	var noEncObj encodedObject
	keyStorage := util.NewKeyStorage(&anyNodeKey, nil, nil)
	localPubKey := anyNodeSigner.PublicKeyBytes

	mainECRules := []iec.Rule{
		{DataPartNum: 3, ParityPartNum: 1},
		{DataPartNum: 6, ParityPartNum: 3},
		{DataPartNum: 9, ParityPartNum: 4},
	}

	nodeLists := allocNodes([]uint{4, 9, 13})

	inPolicy := InitialPlacementPolicy{
		MaxPerRuleReplicas: []uint32{0, 1, 1},
	}

	var clientConns testConnCollector
	var localStorage testLocalStorageCallCollector

	tgt := &distributedTarget{
		placementIterator: placementIterator{
			log:      zap.NewNop(),
			neoFSNet: testNetwork{localPubKey: localPubKey},
		},
		keyStorage:        keyStorage,
		clientConstructor: &clientConns,
		localStorage:      &localStorage,
		sessionSigner:     neofscryptotest.Signer(),
		containerNodes:    mockContainerNodes{},
	}

	err := tgt.handleInitialPlacementPolicy(inPolicy, nil, mainECRules, nodeLists, obj, noEncObj)
	require.NoError(t, err)

	calledNodes := clientConns.collectConns()
	require.Len(t, calledNodes, 22)
	for _, node := range slices.Concat(nodeLists[1], nodeLists[2]) {
		require.Contains(t, calledNodes, node.PublicKey())
	}
}
