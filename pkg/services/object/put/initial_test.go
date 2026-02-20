package putsvc

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

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

func (x *testConnCollector) assertCallGroups(t *testing.T, groups [][][2]int, nodeLists [][]netmap.NodeInfo) {
	sum := 0
	for i := range groups {
		sum += len(groups[i])
	}
	require.Len(t, x.conns, sum)

	conns := x.conns
	for i := range groups {
		gotGroup := conns[:len(groups[i])]
		for _, node := range groups[i] {
			require.Contains(t, gotGroup, nodeLists[node[0]][node[1]].PublicKey(), "node [%d,%d] is not called in group #%d", node[0], node[1], i)
		}
		conns = conns[len(groups[i]):]
	}

	for i := range nodeLists {
		for j := range nodeLists[i] {
			if slices.ContainsFunc(groups, func(group [][2]int) bool { return slices.Contains(group, [2]int{i, j}) }) {
				require.Contains(t, x.conns, nodeLists[i][j].PublicKey(), "node [%d,%d] is not called while should be", i, j)
			} else {
				require.NotContains(t, x.conns, nodeLists[i][j].PublicKey(), "node [%d,%d] is called while should not be", i, j)
			}
		}
	}
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

func TestInitialPlacement_RepRules(t *testing.T) {
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

		for tIdx, tc := range []struct {
			repRules     []uint
			initialRules []uint32
			failedRule   int
			succeeded    int
			left         int
			failNodes    [][]int
			callGroups   [][][2]int
		}{
			// main [3], initial [1]
			//   [fail] [fail] [fail]
			{repRules: []uint{3}, initialRules: []uint32{1}, failedRule: 0, succeeded: 0, left: 0, failNodes: [][]int{
				{0, 1, 2},
			}, callGroups: [][][2]int{{
				{0, 0},
			}, {
				{0, 1},
			}, {
				{0, 2},
			}}},
			// main [3], initial [2]
			//   [fail] [fail] [x]
			{repRules: []uint{3}, initialRules: []uint32{2}, failedRule: 0, succeeded: 0, left: 1, failNodes: [][]int{
				{0, 1},
			}, callGroups: [][][2]int{{
				{0, 0}, {0, 1},
			}}},
			// main [3, 3, 3], initial [1, 1, 1]
			//  [fail] [fail] [ok]
			//  [fail] [fail] [fail]
			//  [fail] [fail] [ok]
			{repRules: []uint{3, 3, 3}, initialRules: []uint32{1, 1, 1}, failedRule: 1, succeeded: 0, left: 0, failNodes: [][]int{
				{0, 1}, {0, 1, 2}, {0, 1},
			}, callGroups: [][][2]int{{
				{0, 0},
				{1, 0},
				{2, 0},
			}, {
				{0, 1},
				{1, 1},
				{2, 1},
			}, {
				{0, 2},
				{1, 2},
				{2, 2},
			}}},
			// main [3, 5, 7] initial [1, 2, 4]
			//   [fail] [fail] [ok]
			//   [ok] [fail] [fail] [fail] [x]
			//   [fail] [ok] [fail] [ok] [fail] [ok] [fail]
			{repRules: []uint{3, 5, 7}, initialRules: []uint32{1, 2, 4}, failedRule: 2, succeeded: 3, left: 0, failNodes: [][]int{
				{0, 1}, {1, 2, 3}, {0, 2, 4, 6},
			}, callGroups: [][][2]int{{
				{0, 0},
				{1, 0}, {1, 1},
				{2, 0}, {2, 1}, {2, 2}, {2, 3},
			}, {
				{0, 1},
				{1, 2},
				{2, 4}, {2, 5},
			}, {
				{0, 2},
				{1, 3},
				{2, 6},
			}}},
		} {
			t.Run(strconv.Itoa(tIdx), func(t *testing.T) {
				nodeLists := allocNodes(tc.repRules)
				plc := InitialPlacementPolicy{
					MaxPerRuleReplicas: tc.initialRules,
				}

				var clientConns testConnCollector
				for listIdx := range tc.failNodes {
					for _, nodeIdx := range tc.failNodes[listIdx] {
						clientConns.failNode(nodeLists[listIdx][nodeIdx])
					}
				}

				tgt.clientConstructor = &clientConns

				err := tgt.putInitial(obj, noEncObj, tc.repRules, nil, nodeLists, plc)
				require.Error(t, err)

				expMsg := fmt.Sprintf("unable to reach REP rule #%d (required: %d, succeeded: %d, left nodes: %d)",
					tc.failedRule, tc.initialRules[tc.failedRule], tc.succeeded, tc.left)
				if tc.succeeded > 0 {
					var e apistatus.Incomplete
					require.ErrorAs(t, err, &e)
					require.Equal(t, expMsg, e.Message())
				} else {
					require.EqualError(t, err, expMsg)
				}

				clientConns.assertCallGroups(t, tc.callGroups, nodeLists)
			})
		}
	})

	t.Run("fault tolerance", func(t *testing.T) {
		// tgt := &distributedTarget{
		// 	placementIterator: placementIterator{
		// 		neoFSNet: testNetwork{},
		// 	},
		// 	keyStorage: keyStorage,
		// }
		//
		// for i, tc := range []struct {
		// 	main      []uint
		// 	initial   []uint32
		// 	failNodes [][]int
		// 	okNodes   [][]int
		// }{
		// 	{main: []uint{3, 3, 3}, initial: []uint32{1, 1, 1}, failNodes: [][]int{
		// 		{0, 1}, {0, 1}, {0, 1},
		// 	}, okNodes: [][]int{
		// 		{2}, {2}, {2},
		// 	}},
		// 	{main: []uint{1, 7, 11}, initial: []uint32{1, 3, 5}, failNodes: [][]int{
		// 		{}, {1, 3}, {0, 2, 4, 5},
		// 	}, okNodes: [][]int{
		// 		{0}, {0, 2, 4}, {1, 3, 6, 7, 8},
		// 	}},
		// } {
		// 	nodeLists := allocNodes(tc.main)
		// 	plc := InitialPlacementPolicy{
		// 		MaxPerRuleReplicas: tc.initial,
		// 	}
		//
		// 	var clientConns testConnCollector
		// 	for listIdx := range tc.failNodes {
		// 		for _, nodeIdx := range tc.failNodes[listIdx] {
		// 			clientConns.failNode(nodeLists[listIdx][nodeIdx])
		// 		}
		// 	}
		//
		// 	l, logBuf := testutil.NewBufferedLogger(t, zap.ErrorLevel)
		//
		// 	tgt.clientConstructor = &clientConns
		// 	tgt.placementIterator.log = l
		//
		// 	err := tgt.putInitial(obj, noEncObj, tc.main, nil, nodeLists, plc)
		// 	require.NoError(t, err, i)
		//
		// 	calledNodes := clientConns.collectConns()
		//
		// 	for listIdx := range nodeLists {
		// 		for nodeIdx, node := range nodeLists[listIdx] {
		// 			if slices.Contains(tc.failNodes[listIdx], nodeIdx) {
		// 				logBuf.AssertContains(testutil.LogEntry{
		// 					Level:   zap.ErrorLevel,
		// 					Message: "initial REP placement to node failed",
		// 					Fields: map[string]any{
		// 						"object": obj.Address().String(), "repRule": json.Number(strconv.Itoa(listIdx)), "nodeIdx": json.Number(strconv.Itoa(nodeIdx)), "local": false,
		// 						"error": fmt.Sprintf("could not close object stream: could not create SDK client %x: [test] forced error", node.PublicKey()),
		// 					},
		// 				})
		// 			} else if !slices.Contains(tc.okNodes[listIdx], nodeIdx) {
		// 				require.NotContains(t, calledNodes, node.PublicKey(), i)
		// 				continue
		// 			}
		//
		// 			require.Contains(t, calledNodes, node.PublicKey(), i)
		// 		}
		// 	}
		// }
	})

	tgt := &distributedTarget{
		placementIterator: placementIterator{
			log:      zap.NewNop(),
			neoFSNet: testNetwork{},
		},
		keyStorage: keyStorage,
	}

	for tIdx, tc := range []struct {
		repRules     []uint
		initialRules []uint32
		callGroups   [][][2]int
	}{
		// main [3], initial [1]
		//   [ok]
		{repRules: []uint{3}, initialRules: []uint32{1}, callGroups: [][][2]int{{
			{0, 0},
		}}},
		// main [3], initial [2]
		//   [ok] [ok] [x]
		{repRules: []uint{3}, initialRules: []uint32{2}, callGroups: [][][2]int{{
			{0, 0}, {0, 1},
		}}},
		// main [3, 3, 3], initial [1, 1, 1]
		//  [ok] [x] [x]
		//  [ok] [x] [x]
		//  [ok] [x] [x]
		{repRules: []uint{3, 3, 3}, initialRules: []uint32{1, 1, 1}, callGroups: [][][2]int{{
			{0, 0},
			{1, 0},
			{2, 0},
		}}},
		// main [3, 5, 7] initial [1, 2, 4]
		//   [ok] [x] [x]
		//   [ok] [ok] [x] [x] [x]
		//   [ok] [ok] [ok] [ok] [x] [x] [x]
		{repRules: []uint{3, 5, 7}, initialRules: []uint32{1, 2, 4}, callGroups: [][][2]int{{
			{0, 0},
			{1, 0}, {1, 1},
			{2, 0}, {2, 1}, {2, 2}, {2, 3},
		}}},
	} {
		t.Run(strconv.Itoa(tIdx), func(t *testing.T) {
			nodeCounts := slices.Clone(tc.repRules)
			for i := range nodeCounts {
				nodeCounts[i] *= 3 // default CBF
			}
			nodeLists := allocNodes(nodeCounts)

			plc := InitialPlacementPolicy{
				MaxPerRuleReplicas: tc.initialRules,
			}

			var clientConns testConnCollector
			tgt.clientConstructor = &clientConns

			err := tgt.putInitial(obj, noEncObj, tc.repRules, nil, nodeLists, plc)
			require.NoError(t, err)

			clientConns.assertCallGroups(t, tc.callGroups, nodeLists)
		})
	}
}

func TestInitialPlacement_MaxReplicas(t *testing.T) {
	// anyNodeKey := neofscryptotest.Signer().ECDSAPrivateKey
	// obj := objecttest.Object()
	// var noEncObj encodedObject
	// keyStorage := util.NewKeyStorage(&anyNodeKey, nil, nil)

	t.Run("failures", func(t *testing.T) {
		// tgt := &distributedTarget{
		// 	placementIterator: placementIterator{
		// 		log:      zap.NewNop(),
		// 		neoFSNet: testNetwork{},
		// 	},
		// 	keyStorage: keyStorage,
		// }
		//
		// for i, tc := range []struct {
		// 	main        []uint
		// 	maxReplicas uint32
		// 	failNodes   [][2]int
		// 	called      [][2]int
		// }{
		// 	{main: []uint{3, 3, 3}, maxReplicas: 1, failNodes: [][2]int{
		// 		{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2}, {1, 2}, {2, 2},
		// 	}, called: [][2]int{
		// 		{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2}, {1, 2}, {2, 2},
		// 	}},
		// 	{main: []uint{3, 3, 3}, maxReplicas: 6, failNodes: [][2]int{
		// 		{0, 0}, {1, 1}, {1, 2}, {2, 2},
		// 	}, called: [][2]int{
		// 		{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2}, {1, 2}, {2, 2},
		// 	}},
		// } {
		// 	nodeLists := allocNodes(tc.main)
		// 	plc := InitialPlacementPolicy{
		// 		MaxReplicas: tc.maxReplicas,
		// 	}
		//
		// 	var clientConns testConnCollector
		// 	for _, failIdx := range tc.failNodes {
		// 		clientConns.failNode(nodeLists[failIdx[0]][failIdx[1]])
		// 	}
		//
		// 	tgt.clientConstructor = &clientConns
		//
		// 	err := tgt.putInitial(obj, noEncObj, tc.main, nil, nodeLists, plc)
		// 	require.Error(t, err)
		//
		// 	leftNodes := islices.TwoDimSliceElementCount(nodeLists) - len(tc.called)
		// 	expErr := fmt.Sprintf("unable to reach MaxReplicas %d (succeeded: %d, left nodes: %d)",
		// 		tc.maxReplicas, len(tc.called)-len(tc.failNodes), leftNodes)
		// 	require.ErrorContains(t, err, expErr, i)
		// 	if len(tc.called)-len(tc.failNodes) > 0 {
		// 		require.ErrorIs(t, err, apistatus.ErrIncomplete, i)
		// 	}
		//
		// 	calledNodes := clientConns.collectConns()
		// 	require.Len(t, calledNodes, len(tc.called))
		// 	for listIdx := range nodeLists {
		// 		for nodeIdx := range nodeLists[listIdx] {
		// 			if slices.Contains(tc.called, [2]int{listIdx, nodeIdx}) {
		// 				require.Contains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
		// 			} else {
		// 				require.NotContains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
		// 			}
		// 		}
		// 	}
		// }
	})

	t.Run("fault tolerance", func(t *testing.T) {
		// tgt := &distributedTarget{
		// 	placementIterator: placementIterator{
		// 		log:      zap.NewNop(),
		// 		neoFSNet: testNetwork{},
		// 	},
		// 	keyStorage: keyStorage,
		// }
		//
		// for i, tc := range []struct {
		// 	main        []uint
		// 	maxReplicas uint32
		// 	failNodes   [][2]int
		// 	called      [][2]int
		// }{
		// 	{main: []uint{3, 3, 3}, maxReplicas: 1, failNodes: [][2]int{
		// 		{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2}, {1, 2},
		// 	}, called: [][2]int{
		// 		{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2}, {1, 2}, {2, 2},
		// 	}},
		// 	{main: []uint{3, 3, 3}, maxReplicas: 6, failNodes: [][2]int{
		// 		{0, 0}, {1, 1}, {1, 2},
		// 	}, called: [][2]int{
		// 		{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2}, {1, 2}, {2, 2},
		// 	}},
		// } {
		// 	nodeLists := allocNodes(tc.main)
		// 	plc := InitialPlacementPolicy{
		// 		MaxReplicas: tc.maxReplicas,
		// 	}
		//
		// 	var clientConns testConnCollector
		// 	for _, failIdx := range tc.failNodes {
		// 		clientConns.failNode(nodeLists[failIdx[0]][failIdx[1]])
		// 	}
		//
		// 	tgt.clientConstructor = &clientConns
		//
		// 	err := tgt.putInitial(obj, noEncObj, tc.main, nil, nodeLists, plc)
		// 	require.NoError(t, err, i)
		//
		// 	calledNodes := clientConns.collectConns()
		// 	require.Len(t, calledNodes, len(tc.called))
		// 	for listIdx := range nodeLists {
		// 		for nodeIdx := range nodeLists[listIdx] {
		// 			if slices.Contains(tc.called, [2]int{listIdx, nodeIdx}) {
		// 				require.Contains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
		// 			} else {
		// 				require.NotContains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
		// 			}
		// 		}
		// 	}
		// }
	})

	// tgt := &distributedTarget{
	// 	placementIterator: placementIterator{
	// 		neoFSNet: testNetwork{},
	// 	},
	// 	keyStorage: keyStorage,
	// }
	//
	// for i, tc := range []struct {
	// 	main        []uint
	// 	maxReplicas uint32
	// 	called      [][2]int
	// }{
	// 	{main: []uint{3, 3, 3}, maxReplicas: 1, called: [][2]int{
	// 		{0, 0},
	// 	}},
	// 	{main: []uint{3, 3, 3}, maxReplicas: 7, called: [][2]int{
	// 		{0, 0}, {1, 0}, {2, 0}, {0, 1}, {1, 1}, {2, 1}, {0, 2},
	// 	}},
	// 	{main: []uint{3, 1, 3}, maxReplicas: 6, called: [][2]int{
	// 		{0, 0}, {1, 0}, {2, 0}, {0, 1}, {2, 1}, {0, 2},
	// 	}},
	// } {
	// 	nodeLists := allocNodes(tc.main)
	// 	plc := InitialPlacementPolicy{
	// 		MaxReplicas: tc.maxReplicas,
	// 	}
	//
	// 	var clientConns testConnCollector
	// 	tgt.clientConstructor = &clientConns
	//
	// 	err := tgt.putInitial(obj, noEncObj, tc.main, nil, nodeLists, plc)
	// 	require.NoError(t, err, i)
	//
	// 	calledNodes := clientConns.collectConns()
	// 	require.Len(t, calledNodes, len(tc.called))
	// 	for listIdx := range nodeLists {
	// 		for nodeIdx := range nodeLists[listIdx] {
	// 			if slices.Contains(tc.called, [2]int{listIdx, nodeIdx}) {
	// 				require.Contains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
	// 			} else {
	// 				require.NotContains(t, calledNodes, nodeLists[listIdx][nodeIdx].PublicKey(), i)
	// 			}
	// 		}
	// 	}
	// }
}

func TestInitialPlacement_MaxReplicas_PreferLocal(t *testing.T) {
	// anyNodeSigner := neofscryptotest.Signer()
	// anyNodeKey := anyNodeSigner.ECDSAPrivateKey
	// obj := objecttest.Object()
	// var noEncObj encodedObject
	// keyStorage := util.NewKeyStorage(&anyNodeKey, nil, nil)
	// localPubKey := anyNodeSigner.PublicKeyBytes
	//
	// mainRepRules := []uint{3, 3, 3}
	//
	// nodeLists := allocNodes(mainRepRules)
	// nodeLists[1][1].SetPublicKey(localPubKey)
	//
	// plc := InitialPlacementPolicy{
	// 	MaxReplicas: 7,
	// 	PreferLocal: true,
	// }
	//
	// var clientConns testConnCollector
	// clientConns.failNode(nodeLists[1][0])
	//
	// var localStorage testLocalStorageCallCollector
	//
	// tgt := &distributedTarget{
	// 	placementIterator: placementIterator{
	// 		log:      zap.NewNop(),
	// 		neoFSNet: testNetwork{localPubKey: localPubKey},
	// 	},
	// 	keyStorage:        keyStorage,
	// 	clientConstructor: &clientConns,
	// 	localStorage:      &localStorage,
	// }
	//
	// err := tgt.putInitial(obj, noEncObj, mainRepRules, nil, nodeLists, plc)
	// require.NoError(t, err)
	//
	// calledNodes := clientConns.collectConns()
	// require.EqualValues(t, 1, localStorage.callCount())
	// require.Len(t, calledNodes, 7)
	// require.Contains(t, calledNodes, nodeLists[1][0].PublicKey())
	// require.Contains(t, calledNodes, nodeLists[1][2].PublicKey())
	// require.Contains(t, calledNodes, nodeLists[0][0].PublicKey())
	// require.Contains(t, calledNodes, nodeLists[2][0].PublicKey())
	// require.Contains(t, calledNodes, nodeLists[0][1].PublicKey())
	// require.Contains(t, calledNodes, nodeLists[2][1].PublicKey())
}

func TestInitialPlacement_EC(t *testing.T) {
	// anyNodeSigner := neofscryptotest.Signer()
	// anyNodeKey := anyNodeSigner.ECDSAPrivateKey
	// obj := objecttest.Object()
	// var noEncObj encodedObject
	// keyStorage := util.NewKeyStorage(&anyNodeKey, nil, nil)
	// localPubKey := anyNodeSigner.PublicKeyBytes
	//
	// mainECRules := []iec.Rule{
	// 	{DataPartNum: 3, ParityPartNum: 1},
	// 	{DataPartNum: 6, ParityPartNum: 3},
	// 	{DataPartNum: 9, ParityPartNum: 4},
	// }
	//
	// nodeLists := allocNodes([]uint{4, 9, 13})
	//
	// plc := InitialPlacementPolicy{
	// 	MaxPerRuleReplicas: []uint32{0, 1, 1},
	// }
	//
	// var clientConns testConnCollector
	// var localStorage testLocalStorageCallCollector
	//
	// tgt := &distributedTarget{
	// 	placementIterator: placementIterator{
	// 		log:      zap.NewNop(),
	// 		neoFSNet: testNetwork{localPubKey: localPubKey},
	// 	},
	// 	keyStorage:        keyStorage,
	// 	clientConstructor: &clientConns,
	// 	localStorage:      &localStorage,
	// 	sessionSigner:     neofscryptotest.Signer(),
	// 	containerNodes:    mockContainerNodes{},
	// }
	//
	// err := tgt.putInitial(obj, noEncObj, nil, mainECRules, nodeLists, plc)
	// require.NoError(t, err)
	//
	// calledNodes := clientConns.collectConns()
	// require.Len(t, calledNodes, 22)
	// for _, node := range slices.Concat(nodeLists[1], nodeLists[2]) {
	// 	require.Contains(t, calledNodes, node.PublicKey())
	// }
}
