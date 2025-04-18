package putsvc

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func allocNodes(n []uint) [][]netmap.NodeInfo {
	res := make([][]netmap.NodeInfo, len(n))
	for i := range res {
		res[i] = make([]netmap.NodeInfo, n[i])
		for j := range res[i] {
			res[i][j].SetPublicKey([]byte(fmt.Sprintf("pub_%d_%d", i, j)))
			res[i][j].SetNetworkEndpoints(
				"localhost:"+strconv.Itoa(1e4+i*100+2*j),
				"localhost:"+strconv.Itoa(1e4+i*100+2*j+1),
			)
		}
	}
	return res
}

type testContainerNodes struct {
	objID oid.ID

	sortErr  error
	cnrNodes [][]netmap.NodeInfo

	primCounts []uint
}

func (x testContainerNodes) Unsorted() [][]netmap.NodeInfo { return x.cnrNodes }

func (x testContainerNodes) SortForObject(obj oid.ID) ([][]netmap.NodeInfo, error) {
	if x.objID != obj {
		return nil, errors.New("[test] unexpected object ID")
	}
	return x.cnrNodes, x.sortErr
}

func (x testContainerNodes) PrimaryCounts() []uint { return x.primCounts }

type testNetwork struct {
	localPubKey []byte
}

func (x testNetwork) IsLocalNodePublicKey(pk []byte) bool { return bytes.Equal(x.localPubKey, pk) }

func (x testNetwork) GetContainerNodes(cid.ID) (ContainerNodes, error) { panic("unimplemented") }
func (x testNetwork) GetEpochBlock(uint64) (uint32, error)             { panic("unimplemented") }

type testWorkerPool struct {
	nCalls int
	nFail  int
	err    error
}

func (x *testWorkerPool) Release() {}
func (x *testWorkerPool) Submit(f func()) error {
	x.nCalls++
	if x.err != nil && (x.nFail == 0 || x.nCalls == x.nFail) {
		return x.err
	}
	go f()
	return nil
}
func (x *testWorkerPool) Tune(_ int) {}

func TestIterateNodesForObject(t *testing.T) {
	// nodes: [A B C] [D C E F G] [B H I J]
	// policy: [2 3 2]
	// C is local. B, E and H fail, all others succeed
	//
	// expected order of candidates: [A B C] [D E F] [H I J]
	objID := oidtest.ID()
	cnrNodes := allocNodes([]uint{3, 5, 4})
	cnrNodes[2][0].SetPublicKey(cnrNodes[0][1].PublicKey())
	cnrNodes[1][1].SetPublicKey(cnrNodes[0][2].PublicKey())
	var lwp, rwp testWorkerPool
	iter := placementIterator{
		log: zap.NewNop(),
		neoFSNet: testNetwork{
			localPubKey: cnrNodes[0][2].PublicKey(),
		},
		remotePool: &rwp,
		localPool:  &lwp,
		containerNodes: testContainerNodes{
			objID:      objID,
			cnrNodes:   cnrNodes,
			primCounts: []uint{2, 3, 2},
		},
	}
	var handlerMtx sync.Mutex
	var handlerCalls []nodeDesc
	err := iter.iterateNodesForObject(objID, func(node nodeDesc) error {
		handlerMtx.Lock()
		handlerCalls = append(handlerCalls, node)
		handlerMtx.Unlock()
		if bytes.Equal(node.info.PublicKey(), cnrNodes[0][1].PublicKey()) ||
			bytes.Equal(node.info.PublicKey(), cnrNodes[1][2].PublicKey()) ||
			bytes.Equal(node.info.PublicKey(), cnrNodes[2][1].PublicKey()) {
			return errors.New("any node error")
		}
		return nil
	})
	require.NoError(t, err)
	require.Len(t, handlerCalls, 9)
	withLocal := false
	for _, node := range handlerCalls[:3] {
		if !withLocal {
			withLocal = node.local
		}
		if node.local {
			continue
		}
		require.Contains(t, [][]byte{
			cnrNodes[0][0].PublicKey(), cnrNodes[0][1].PublicKey(),
		}, node.info.PublicKey())
		require.Len(t, node.info.AddressGroup(), 2)
		var expNetAddrs []string
		if bytes.Equal(node.info.PublicKey(), cnrNodes[0][0].PublicKey()) {
			expNetAddrs = []string{"localhost:10000", "localhost:10001"}
		} else {
			expNetAddrs = []string{"localhost:10002", "localhost:10003"}
		}
		require.ElementsMatch(t, expNetAddrs, []string{node.info.AddressGroup()[0].URIAddr(), node.info.AddressGroup()[1].URIAddr()})
	}
	require.True(t, withLocal)
	for _, node := range handlerCalls[3:6] {
		require.False(t, node.local)
		require.Contains(t, [][]byte{
			cnrNodes[1][0].PublicKey(), cnrNodes[1][2].PublicKey(), cnrNodes[1][3].PublicKey(),
		}, node.info.PublicKey())
		require.Len(t, node.info.AddressGroup(), 2)
		var expNetAddrs []string
		switch key := node.info.PublicKey(); {
		case bytes.Equal(key, cnrNodes[1][0].PublicKey()):
			expNetAddrs = []string{"localhost:10100", "localhost:10101"}
		case bytes.Equal(key, cnrNodes[1][2].PublicKey()):
			expNetAddrs = []string{"localhost:10104", "localhost:10105"}
		case bytes.Equal(key, cnrNodes[1][3].PublicKey()):
			expNetAddrs = []string{"localhost:10106", "localhost:10107"}
		}
		require.ElementsMatch(t, expNetAddrs, []string{node.info.AddressGroup()[0].URIAddr(), node.info.AddressGroup()[1].URIAddr()})
	}
	for _, node := range handlerCalls[6:] {
		require.False(t, node.local)
		require.Contains(t, [][]byte{
			cnrNodes[2][1].PublicKey(), cnrNodes[2][2].PublicKey(), cnrNodes[2][3].PublicKey(),
		}, node.info.PublicKey())
		require.Len(t, node.info.AddressGroup(), 2)
		var expNetAddrs []string
		switch key := node.info.PublicKey(); {
		case bytes.Equal(key, cnrNodes[2][1].PublicKey()):
			expNetAddrs = []string{"localhost:10202", "localhost:10203"}
		case bytes.Equal(key, cnrNodes[2][2].PublicKey()):
			expNetAddrs = []string{"localhost:10204", "localhost:10205"}
		case bytes.Equal(key, cnrNodes[2][3].PublicKey()):
			expNetAddrs = []string{"localhost:10206", "localhost:10207"}
		}
		require.ElementsMatch(t, expNetAddrs, []string{node.info.AddressGroup()[0].URIAddr(), node.info.AddressGroup()[1].URIAddr()})
	}

	t.Run("local only", func(t *testing.T) {
		objID := oidtest.ID()
		cnrNodes := allocNodes([]uint{2, 3, 1})
		var lwp, rwp testWorkerPool
		iter := placementIterator{
			log: zap.NewNop(),
			neoFSNet: testNetwork{
				localPubKey: cnrNodes[1][1].PublicKey(),
			},
			remotePool: &rwp,
			localPool:  &lwp,
			containerNodes: testContainerNodes{
				cnrNodes: cnrNodes,
			},
			localOnly:    true,
			localNodePos: [2]int{1, 1},
			broadcast:    true,
		}
		var handlerMtx sync.Mutex
		var handlerCalls []nodeDesc
		err := iter.iterateNodesForObject(objID, func(node nodeDesc) error {
			handlerMtx.Lock()
			handlerCalls = append(handlerCalls, node)
			handlerMtx.Unlock()
			return nil
		})
		require.NoError(t, err)
		require.Len(t, handlerCalls, 1)
		require.True(t, handlerCalls[0].local)
		require.EqualValues(t, 1, lwp.nCalls)
		require.Zero(t, rwp.nCalls)
	})
	t.Run("linear num of replicas", func(t *testing.T) {
		// nodes: [A B C] [D B E] [F G]
		// policy: [2 1 2]
		// B fails, all others succeed
		// linear num: 4
		//
		// expected order of candidates: [A B C D E]
		objID := oidtest.ID()
		cnrNodes := allocNodes([]uint{3, 3, 2})
		cnrNodes[1][1].SetPublicKey(cnrNodes[0][1].PublicKey())
		iter := placementIterator{
			log:        zap.NewNop(),
			neoFSNet:   new(testNetwork),
			remotePool: new(testWorkerPool),
			localPool:  new(testWorkerPool),
			containerNodes: testContainerNodes{
				objID:      objID,
				cnrNodes:   cnrNodes,
				primCounts: []uint{2, 1, 2},
			},
			linearReplNum: 4,
		}
		var handlerMtx sync.Mutex
		var handlerCalls [][]byte
		err := iter.iterateNodesForObject(objID, func(node nodeDesc) error {
			handlerMtx.Lock()
			handlerCalls = append(handlerCalls, node.info.PublicKey())
			handlerMtx.Unlock()
			if bytes.Equal(node.info.PublicKey(), cnrNodes[0][1].PublicKey()) {
				return errors.New("any node error")
			}
			return nil
		})
		require.NoError(t, err)
		require.Len(t, handlerCalls, 5)
		require.ElementsMatch(t, [][]byte{
			cnrNodes[0][0].PublicKey(),
			cnrNodes[0][1].PublicKey(),
			cnrNodes[0][2].PublicKey(),
			cnrNodes[1][0].PublicKey(),
			cnrNodes[1][2].PublicKey(),
		}, handlerCalls)
	})
	t.Run("broadcast", func(t *testing.T) {
		// nodes: [A B] [C D B] [E F]
		// policy: [1 1 1]
		// D fails, all others succeed
		//
		// expected order candidates: [A C E B D F]
		objID := oidtest.ID()
		cnrNodes := allocNodes([]uint{2, 3, 2})
		cnrNodes[1][2].SetPublicKey(cnrNodes[0][1].PublicKey())
		iter := placementIterator{
			log:        zap.NewNop(),
			neoFSNet:   new(testNetwork),
			remotePool: new(testWorkerPool),
			localPool:  new(testWorkerPool),
			containerNodes: testContainerNodes{
				objID:      objID,
				cnrNodes:   cnrNodes,
				primCounts: []uint{1, 1, 1},
			},
			broadcast: true,
		}
		var handlerMtx sync.Mutex
		var handlerCalls [][]byte
		err := iter.iterateNodesForObject(objID, func(node nodeDesc) error {
			handlerMtx.Lock()
			handlerCalls = append(handlerCalls, node.info.PublicKey())
			handlerMtx.Unlock()
			if bytes.Equal(node.info.PublicKey(), cnrNodes[1][1].PublicKey()) {
				return errors.New("any node error")
			}
			return nil
		})
		require.NoError(t, err)
		require.Len(t, handlerCalls, 6)
		require.ElementsMatch(t, [][]byte{
			cnrNodes[0][0].PublicKey(),
			cnrNodes[1][0].PublicKey(),
			cnrNodes[2][0].PublicKey(),
		}, handlerCalls[:3])
		for _, key := range handlerCalls[3:] {
			require.Contains(t, [][]byte{
				cnrNodes[0][1].PublicKey(),
				cnrNodes[1][1].PublicKey(),
				cnrNodes[2][1].PublicKey(),
			}, key)
		}
	})
	t.Run("sort nodes for object failure", func(t *testing.T) {
		objID := oidtest.ID()
		iter := placementIterator{
			log: zap.NewNop(),
			containerNodes: testContainerNodes{
				objID:   objID,
				sortErr: errors.New("any sort error"),
			},
		}
		err := iter.iterateNodesForObject(objID, func(nodeDesc) error {
			t.Fatal("must not be called")
			return nil
		})
		require.EqualError(t, err, "sort container nodes for the object: any sort error")
	})
	t.Run("worker pool failure", func(t *testing.T) {
		// nodes: [A B] [C D E] [F]
		// policy: [2 2 1]
		// C fails network op, worker pool fails for E
		objID := oidtest.ID()
		replCounts := []uint{2, 3, 1}
		cnrNodes := allocNodes(replCounts)
		wp := testWorkerPool{
			nFail: 5,
			err:   errors.New("any worker pool error"),
		}
		iter := placementIterator{
			log:        zap.NewNop(),
			neoFSNet:   new(testNetwork),
			localPool:  &wp,
			remotePool: &wp,
			containerNodes: testContainerNodes{
				objID:      objID,
				cnrNodes:   cnrNodes,
				primCounts: []uint{2, 2, 1},
			},
		}
		var handlerMtx sync.Mutex
		var handlerCalls [][]byte
		err := iter.iterateNodesForObject(objID, func(node nodeDesc) error {
			handlerMtx.Lock()
			handlerCalls = append(handlerCalls, node.info.PublicKey())
			handlerMtx.Unlock()
			if bytes.Equal(node.info.PublicKey(), cnrNodes[1][0].PublicKey()) {
				return errors.New("any node error")
			}
			return nil
		})
		require.Len(t, handlerCalls, 4)
		require.ElementsMatch(t, handlerCalls[:2], [][]byte{
			cnrNodes[0][0].PublicKey(), cnrNodes[0][1].PublicKey(),
		})
		require.ElementsMatch(t, handlerCalls[2:], [][]byte{
			cnrNodes[1][0].PublicKey(), cnrNodes[1][1].PublicKey(),
		})
		require.EqualError(t, err, "incomplete object PUT by placement: "+
			"number of replicas cannot be met for list #1: 1 required, 0 nodes remaining (last node error: any node error)")
	})
	t.Run("not enough nodes a priori", func(t *testing.T) {
		// nodes: [A B] [C D E] [F]
		// policy: [2 4 1]
		//
		// after 1st list is finished, 2nd list has not enough nodes, so service should
		// not try to access them
		//
		// this test is a bit synthetic. Actually, selection of container nodes must
		// fail if there is not enough nodes. But for complete coverage, detection of
		// such cases is also tested
		objID := oidtest.ID()
		cnrNodes := allocNodes([]uint{2, 3, 1})
		var wp testWorkerPool
		iter := placementIterator{
			log:        zap.NewNop(),
			neoFSNet:   new(testNetwork),
			remotePool: &wp,
			containerNodes: testContainerNodes{
				objID:      objID,
				cnrNodes:   cnrNodes,
				primCounts: []uint{2, 4, 1},
			},
		}
		var handlerMtx sync.Mutex
		var handlerCalls [][]byte
		err := iter.iterateNodesForObject(objID, func(node nodeDesc) error {
			handlerMtx.Lock()
			handlerCalls = append(handlerCalls, node.info.PublicKey())
			handlerMtx.Unlock()
			if bytes.Equal(node.info.PublicKey(), cnrNodes[1][0].PublicKey()) {
				return errors.New("any node error")
			}
			return nil
		})
		require.EqualError(t, err, "incomplete object PUT by placement: "+
			"number of replicas cannot be met for list #1: 4 required, 3 nodes remaining")
		// assert that we processed 1st list and did not even try to access
		// nodes from the 2nd one
		require.Len(t, handlerCalls, 2)
		require.ElementsMatch(t, handlerCalls, [][]byte{
			cnrNodes[0][0].PublicKey(), cnrNodes[0][1].PublicKey(),
		})
	})
	t.Run("not enough nodes due to decoding network endpoints failure", func(t *testing.T) {
		// nodes: [A B] [C D E] [F]
		// policy: [2 2 1]
		// node C fails network op, E has incorrect network endpoints. Service should try it but fail
		objID := oidtest.ID()
		cnrNodes := allocNodes([]uint{2, 3, 1})
		cnrNodes[1][2].SetNetworkEndpoints("definitely invalid network address")
		var wp testWorkerPool
		iter := placementIterator{
			log:        zap.NewNop(),
			neoFSNet:   new(testNetwork),
			remotePool: &wp,
			containerNodes: testContainerNodes{
				objID:      objID,
				cnrNodes:   cnrNodes,
				primCounts: []uint{2, 2, 1},
			},
		}
		var handlerMtx sync.Mutex
		var handlerCalls [][]byte
		err := iter.iterateNodesForObject(objID, func(node nodeDesc) error {
			handlerMtx.Lock()
			handlerCalls = append(handlerCalls, node.info.PublicKey())
			handlerMtx.Unlock()
			if bytes.Equal(node.info.PublicKey(), cnrNodes[1][0].PublicKey()) {
				return errors.New("any node error")
			}
			return nil
		})
		require.ErrorContains(t, err, "incomplete object PUT by placement: "+
			"number of replicas cannot be met for list #1: 1 required, 0 nodes remaining "+
			"(last node error: failed to decode network addresses:")
		// assert that we processed 1st list and did not even try to access
		// nodes from the 2nd one
		require.Len(t, handlerCalls, 4)
		require.ElementsMatch(t, handlerCalls[:2], [][]byte{
			cnrNodes[0][0].PublicKey(), cnrNodes[0][1].PublicKey(),
		})
		require.ElementsMatch(t, handlerCalls[2:], [][]byte{
			cnrNodes[1][0].PublicKey(), cnrNodes[1][1].PublicKey(),
		})
	})
	t.Run("not enough nodes left after RPC failures", func(t *testing.T) {
		// nodes: [A B] [C D E] [F]
		// policy: [2 3 1]
		// C and D fail network op
		objID := oidtest.ID()
		cnrNodes := allocNodes([]uint{2, 3, 1})
		var wp testWorkerPool
		iter := placementIterator{
			log:        zap.NewNop(),
			neoFSNet:   new(testNetwork),
			remotePool: &wp,
			containerNodes: testContainerNodes{
				objID:      objID,
				cnrNodes:   cnrNodes,
				primCounts: []uint{2, 2, 1},
			},
		}
		var handlerMtx sync.Mutex
		var handlerCalls [][]byte
		err := iter.iterateNodesForObject(objID, func(node nodeDesc) error {
			handlerMtx.Lock()
			handlerCalls = append(handlerCalls, node.info.PublicKey())
			handlerMtx.Unlock()
			if bytes.Equal(node.info.PublicKey(), cnrNodes[1][0].PublicKey()) ||
				bytes.Equal(node.info.PublicKey(), cnrNodes[1][1].PublicKey()) {
				return errors.New("any node error")
			}
			return nil
		})
		require.EqualError(t, err, "incomplete object PUT by placement: "+
			"number of replicas cannot be met for list #1: 2 required, 1 nodes remaining "+
			"(last node error: any node error)")
		require.Len(t, handlerCalls, 4)
		require.ElementsMatch(t, handlerCalls[:2], [][]byte{
			cnrNodes[0][0].PublicKey(), cnrNodes[0][1].PublicKey(),
		})
		require.ElementsMatch(t, handlerCalls[2:], [][]byte{
			cnrNodes[1][0].PublicKey(), cnrNodes[1][1].PublicKey(),
		})
	})
	t.Run("return only after worker pool finished", func(t *testing.T) {
		objID := oidtest.ID()
		cnrNodes := allocNodes([]uint{2, 3, 1})
		iter := placementIterator{
			log:      zap.NewNop(),
			neoFSNet: new(testNetwork),
			remotePool: &testWorkerPool{
				err:   errors.New("pool err"),
				nFail: 2,
			},
			containerNodes: testContainerNodes{
				objID:      objID,
				cnrNodes:   cnrNodes,
				primCounts: []uint{2, 3, 1},
			},
		}
		blockCh := make(chan struct{})
		returnCh := make(chan struct{})
		go func() {
			err := iter.iterateNodesForObject(objID, func(node nodeDesc) error {
				<-blockCh
				return nil
			})
			require.EqualError(t, err, "incomplete object PUT by placement: number of replicas cannot be met for list #0: 1 required, 0 nodes remaining")
			close(returnCh)
		}()
		select {
		case <-returnCh:
			t.Fatal("`iterateNodesForObject` is not synced with worker pools")
		case <-time.After(time.Second / 2):
		}
		close(blockCh)
		select {
		case <-returnCh:
		case <-time.After(10 * time.Second):
			t.Fatal("unexpected test lock")
		}
	})
}
