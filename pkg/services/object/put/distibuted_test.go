package putsvc

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/services/meta"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func allocNodes(n []uint) [][]netmap.NodeInfo {
	res := make([][]netmap.NodeInfo, len(n))
	for i := range res {
		res[i] = make([]netmap.NodeInfo, n[i])
		for j := range res[i] {
			res[i][j].SetPublicKey(fmt.Appendf(nil, "pub_%d_%d", i, j))
			res[i][j].SetNetworkEndpoints(
				"localhost:"+strconv.Itoa(1e4+i*100+2*j),
				"localhost:"+strconv.Itoa(1e4+i*100+2*j+1),
			)
		}
	}
	return res
}

type testNetwork struct {
	localPubKey []byte
}

func (x testNetwork) IsLocalNodePublicKey(pk []byte) bool { return bytes.Equal(x.localPubKey, pk) }

func (x testNetwork) GetContainerNodes(cid.ID) (ContainerNodes, error) { panic("unimplemented") }
func (x testNetwork) GetEpochBlock(uint64) (uint32, error)             { panic("unimplemented") }
func (x testNetwork) GetEpochBlockByTime(uint32) (uint32, error)       { panic("unimplemented") }

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
	iter := placementIterator{
		log: zaptest.NewLogger(t),
		neoFSNet: testNetwork{
			localPubKey: cnrNodes[0][2].PublicKey(),
		},
	}
	var handlerMtx sync.Mutex
	var handlerCalls []nodeDesc
	err := iter.iterateNodesForObject(objID, []uint{2, 3, 2}, cnrNodes, false, func(node nodeDesc) error {
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
		var expNetAddrs []string
		if bytes.Equal(node.info.PublicKey(), cnrNodes[0][0].PublicKey()) {
			expNetAddrs = []string{"localhost:10000", "localhost:10001"}
		} else {
			expNetAddrs = []string{"localhost:10002", "localhost:10003"}
		}
		require.ElementsMatch(t, expNetAddrs, slices.Collect(node.info.NetworkEndpoints()))
	}
	require.True(t, withLocal)
	for _, node := range handlerCalls[3:6] {
		require.False(t, node.local)
		require.Contains(t, [][]byte{
			cnrNodes[1][0].PublicKey(), cnrNodes[1][2].PublicKey(), cnrNodes[1][3].PublicKey(),
		}, node.info.PublicKey())
		var expNetAddrs []string
		switch key := node.info.PublicKey(); {
		case bytes.Equal(key, cnrNodes[1][0].PublicKey()):
			expNetAddrs = []string{"localhost:10100", "localhost:10101"}
		case bytes.Equal(key, cnrNodes[1][2].PublicKey()):
			expNetAddrs = []string{"localhost:10104", "localhost:10105"}
		case bytes.Equal(key, cnrNodes[1][3].PublicKey()):
			expNetAddrs = []string{"localhost:10106", "localhost:10107"}
		}
		require.ElementsMatch(t, expNetAddrs, slices.Collect(node.info.NetworkEndpoints()))
	}
	for _, node := range handlerCalls[6:] {
		require.False(t, node.local)
		require.Contains(t, [][]byte{
			cnrNodes[2][1].PublicKey(), cnrNodes[2][2].PublicKey(), cnrNodes[2][3].PublicKey(),
		}, node.info.PublicKey())
		var expNetAddrs []string
		switch key := node.info.PublicKey(); {
		case bytes.Equal(key, cnrNodes[2][1].PublicKey()):
			expNetAddrs = []string{"localhost:10202", "localhost:10203"}
		case bytes.Equal(key, cnrNodes[2][2].PublicKey()):
			expNetAddrs = []string{"localhost:10204", "localhost:10205"}
		case bytes.Equal(key, cnrNodes[2][3].PublicKey()):
			expNetAddrs = []string{"localhost:10206", "localhost:10207"}
		}
		require.ElementsMatch(t, expNetAddrs, slices.Collect(node.info.NetworkEndpoints()))
	}

	t.Run("broadcast", func(t *testing.T) {
		// nodes: [A B] [C D B] [E F]
		// policy: [1 1 1]
		// D fails, all others succeed
		//
		// every node is asked once, in any order
		objID := oidtest.ID()
		cnrNodes := allocNodes([]uint{2, 3, 2})
		cnrNodes[1][2].SetPublicKey(cnrNodes[0][1].PublicKey())
		iter := placementIterator{
			log:      zaptest.NewLogger(t),
			neoFSNet: new(testNetwork),
		}
		var handlerMtx sync.Mutex
		var handlerCalls [][]byte
		err := iter.iterateNodesForObject(objID, []uint{1, 1, 1}, cnrNodes, true, func(node nodeDesc) error {
			handlerMtx.Lock()
			handlerCalls = append(handlerCalls, node.info.PublicKey())
			handlerMtx.Unlock()
			if bytes.Equal(node.info.PublicKey(), cnrNodes[1][1].PublicKey()) {
				return errors.New("any node error")
			}
			return nil
		})
		require.NoError(t, err)
		require.ElementsMatch(t, [][]byte{
			cnrNodes[0][0].PublicKey(),
			cnrNodes[0][1].PublicKey(),
			cnrNodes[1][0].PublicKey(),
			cnrNodes[1][1].PublicKey(),
			cnrNodes[2][0].PublicKey(),
			cnrNodes[2][1].PublicKey(),
		}, handlerCalls)
	})
	t.Run("broadcast is one round", func(t *testing.T) {
		// nodes: [A B C D]
		// policy: [2]
		//
		// No node finishes until all four have been asked: nodes beyond
		// the policy's two must not wait for the first two to answer.
		const nodes = 4
		objID := oidtest.ID()
		cnrNodes := allocNodes([]uint{nodes})
		iter := placementIterator{
			log:      zaptest.NewLogger(t),
			neoFSNet: new(testNetwork),
		}
		var asked sync.WaitGroup
		asked.Add(nodes)
		allAsked := make(chan struct{})
		go func() { asked.Wait(); close(allAsked) }()
		var late atomic.Bool
		err := iter.iterateNodesForObject(objID, []uint{2}, cnrNodes, true, func(nodeDesc) error {
			asked.Done()
			select {
			case <-allAsked:
			case <-time.After(10 * time.Second):
				late.Store(true)
			}
			return nil
		})
		require.NoError(t, err)
		require.False(t, late.Load(), "a node was asked only after others had answered")
	})
	t.Run("broadcast keeps the policy's minimum", func(t *testing.T) {
		// nodes: [A B C D]
		// policy: [2]
		objID := oidtest.ID()
		cnrNodes := allocNodes([]uint{4})
		iter := placementIterator{
			log:      zaptest.NewLogger(t),
			neoFSNet: new(testNetwork),
		}
		failing := func(keys ...[]byte) func(nodeDesc) error {
			return func(node nodeDesc) error {
				for _, k := range keys {
					if bytes.Equal(node.info.PublicKey(), k) {
						return errors.New("any node error")
					}
				}
				return nil
			}
		}
		// Two of four took the object, whichever two: enough.
		require.NoError(t, iter.iterateNodesForObject(objID, []uint{2}, cnrNodes, true,
			failing(cnrNodes[0][0].PublicKey(), cnrNodes[0][1].PublicKey())))
		// One of four: not enough.
		err := iter.iterateNodesForObject(objID, []uint{2}, cnrNodes, true,
			failing(cnrNodes[0][0].PublicKey(), cnrNodes[0][1].PublicKey(), cnrNodes[0][3].PublicKey()))
		require.ErrorContains(t, err, "any node error")
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
		iter := placementIterator{
			log:      zaptest.NewLogger(t),
			neoFSNet: new(testNetwork),
		}
		var handlerMtx sync.Mutex
		var handlerCalls [][]byte
		err := iter.iterateNodesForObject(objID, []uint{2, 4, 1}, cnrNodes, false, func(node nodeDesc) error {
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
	t.Run("not enough nodes left after RPC failures", func(t *testing.T) {
		// nodes: [A B] [C D E] [F]
		// policy: [2 3 1]
		// C and D fail network op
		objID := oidtest.ID()
		cnrNodes := allocNodes([]uint{2, 3, 1})
		iter := placementIterator{
			log:      zaptest.NewLogger(t),
			neoFSNet: new(testNetwork),
		}
		var handlerMtx sync.Mutex
		var handlerCalls [][]byte
		err := iter.iterateNodesForObject(objID, []uint{2, 2, 1}, cnrNodes, false, func(node nodeDesc) error {
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
}

// A broadcast object is answered by every node of a placement vector, and
// the chain takes exactly as many signatures per vector as the policy asks
// for.
func TestMetaCollection_AddSignature(t *testing.T) {
	m := &metaCollection{
		signatures:    make([][]meta.IndexedSignature, 2),
		maxSignatures: []int{2, 3},
	}
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			m.addSignature(0, meta.IndexedSignature{Index: uint8(i)})
			m.addSignature(1, meta.IndexedSignature{Index: uint8(i)})
		})
	}
	wg.Wait()
	require.Len(t, m.signatures[0], 2)
	require.Len(t, m.signatures[1], 3)

	// Each kept signature is of another node.
	for _, sigs := range m.signatures {
		seen := map[uint8]struct{}{}
		for _, sig := range sigs {
			seen[sig.Index] = struct{}{}
		}
		require.Len(t, seen, len(sigs))
	}
}
