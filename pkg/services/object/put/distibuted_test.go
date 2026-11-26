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

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/services/meta"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
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

func TestMinECPartNum(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metaAttr string
		ruleIdx  int
		want     uint32
	}{
		{name: "configured minimum", want: 3},
		{name: "missing rule", ruleIdx: 1},
		{name: "strict metadata honors minimum", metaAttr: "strict", want: 3},
		{name: "optimistic metadata honors minimum", metaAttr: "optimistic", want: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := distributedTarget{ecMinPartNums: []uint32{3}, metainfoConsistencyAttr: tc.metaAttr}
			require.Equal(t, tc.want, target.minECPartNum(tc.ruleIdx))
		})
	}
}

func TestMetadataSignatures(t *testing.T) {
	for _, tc := range []struct {
		stored int
		want   int
	}{
		{stored: 13, want: 13},
		{stored: 14, want: 14},
		{stored: 15, want: 14},
		{stored: 16, want: 14},
	} {
		t.Run(fmt.Sprintf("%d stored parts", tc.stored), func(t *testing.T) {
			collected := [][]meta.IndexedSignature{
				{{Index: 1}, {Index: 3}}, // REP vector
				make([]meta.IndexedSignature, tc.stored),
				{{Index: 2}, {Index: 4}, {Index: 7}}, // EC without a minimum
			}
			for i := range collected[1] {
				collected[1][i].Index = uint8(i)
			}
			target := distributedTarget{
				containerNodes: mockContainerNodes{repCounts: []uint{2}},
				ecMinPartNums:  []uint32{14, 0},
				metaCollection: &metaCollection{signatures: collected},
			}
			target.metaCollection.signaturesMtx.RLock()
			defer target.metaCollection.signaturesMtx.RUnlock()
			got := target.metadataSignatures()
			require.Equal(t, collected[0], got[0])
			require.Equal(t, collected[1][:tc.want], got[1])
			require.Equal(t, collected[2], got[2])
			require.Len(t, collected[1], tc.stored)
		})
	}
}

func TestInitialPlacementDiscardsSignaturesFromFailedECRule(t *testing.T) {
	rule := iec.Rule{DataPartNum: 2, ParityPartNum: 1}
	rules := []iec.Rule{rule, rule}
	signer := usertest.User()
	obj := *object.New(cid.ID{}, signer.ID)
	obj.SetPayload([]byte("initial EC placement"))
	parts, _, err := iec.Encode(rule, obj.Payload())
	require.NoError(t, err)

	nodeLists := allocNodes([]uint{3, 3})
	var initial netmap.InitialPlacementPolicy
	initial.SetMaxReplicas(1)
	collected := &metaCollection{signatures: make([][]meta.IndexedSignature, len(rules))}
	var collectedCounts [2]int
	firstStored := make(chan struct{})
	target := distributedTarget{
		obj:                     &obj,
		containerNodes:          mockContainerNodes{sorted: nodeLists, ecRules: rules},
		ecPart:                  iec.PartInfo{RuleIndex: -1},
		encodedECParts:          [][][]byte{parts, parts},
		ecMinPartNums:           []uint32{2, 2},
		initialPolicy:           &initial,
		sessionSigner:           signer.RFC6979,
		metaCollection:          collected,
		postPlacementReplicator: mockPostPlacementReplicator{},
		placementIterator:       placementIterator{log: zaptest.NewLogger(t), neoFSNet: testNetwork{}},
	}
	// Emulate signature collection from successful remote writes without
	// submitting a transaction. The first rule can only store one part.
	target.relay = func(node nodeDesc) error {
		i := node.placementVector
		idx := slices.IndexFunc(nodeLists[i], func(n netmap.NodeInfo) bool {
			return bytes.Equal(n.PublicKey(), node.info.PublicKey())
		})
		if i == 0 && idx != 0 {
			<-firstStored
			return errors.New("node unavailable")
		}
		collected.signaturesMtx.Lock()
		defer collected.signaturesMtx.Unlock()
		collectedCounts[i]++
		collected.signatures[i] = append(collected.signatures[i], meta.IndexedSignature{Index: uint8(idx)})
		if i == 0 {
			close(firstStored)
		}
		return nil
	}

	require.NoError(t, target.saveObject(obj, encodedObject{}))
	require.Equal(t, [2]int{1, 3}, collectedCounts)
	collected.signaturesMtx.RLock()
	defer collected.signaturesMtx.RUnlock()
	witness := target.metadataSignatures()
	require.Empty(t, witness[0])
	require.Len(t, witness[1], 2)
}

func TestECProgressResult(t *testing.T) {
	placementErr := fmt.Errorf("save part: %w", errors.Join(errECPartPlacement, errors.New("node unavailable")))
	fatalErr := errors.New("cannot form part")
	for _, tc := range []struct {
		name       string
		success    int32
		minParts   uint32
		incomplete bool
		failed     bool
		fatal      bool
		stopped    bool
	}{
		{name: "all parts", success: 16, minParts: 14},
		{name: "minimum exceeded", success: 15, minParts: 14},
		{name: "minimum met", success: 14, minParts: 14},
		{name: "minimum not met", success: 13, minParts: 14, incomplete: true},
		{name: "data parts only", success: 12, minParts: 14, incomplete: true},
		{name: "below data parts", success: 11, minParts: 14, failed: true},
		{name: "below data parts without early stop", success: 11, failed: true},
		{name: "below data parts with early stop", success: 11, minParts: 14, failed: true, stopped: true},
		{name: "no parts", minParts: 14, failed: true},
		{name: "default requires all", success: 15, incomplete: true},
		{name: "default all parts", success: 16},
		{name: "fatal error is not hidden", success: 14, minParts: 14, fatal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newECProgress(nil, iec.Rule{DataPartNum: 12, ParityPartNum: 4})
			p.successPuts.Store(tc.success)
			p.stop = tc.stopped
			partErr := make([]error, 16)
			for i := int(tc.success); i < len(partErr); i++ {
				partErr[i] = placementErr
			}
			if tc.fatal {
				partErr[len(partErr)-1] = fatalErr
			}
			err := p.result(partErr, tc.minParts)
			if tc.incomplete {
				require.ErrorIs(t, err, apistatus.ErrIncomplete)
			} else if tc.fatal {
				require.ErrorIs(t, err, fatalErr)
			} else if tc.failed {
				require.ErrorContains(t, err, "not enough nodes for EC parts")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

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

// A broadcast object is answered by every node of a placement vector, while
// the placement policy requires a fixed number of signatures per vector.
func TestMetaCollection_AddSignature(t *testing.T) {
	t.Run("cap per vector", func(t *testing.T) {
		m := &metaCollection{
			signatures:    make([][]meta.IndexedSignature, 2),
			maxSignatures: []int{2, 3},
		}
		var wg sync.WaitGroup
		for i := range 4 {
			wg.Go(func() {
				m.addSignature(0, uint8(i), nil, neofscrypto.Signature{}, false)
				m.addSignature(1, uint8(i), nil, neofscrypto.Signature{}, false)
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
	})

	t.Run("overlapping vectors on broadcast", func(t *testing.T) {
		// Vector 0: [A B C] (REP 1), vector 1: [B C D] (REP 2).
		// In one-round broadcast, vector 0 asks A, B and C at once, so
		// vector 1 reuses B and C from nodeResults and only sends to D.
		// Vector 1 still needs 2 distinct signatures with its own indices.
		cnrNodes := allocNodes([]uint{3, 3})
		cnrNodes[1][0].SetPublicKey(cnrNodes[0][1].PublicKey()) // B
		cnrNodes[1][1].SetPublicKey(cnrNodes[0][2].PublicKey()) // C

		// Use different node order in sortedNodes than in cnrNodes so
		// that indices in vector 0 and vector 1 differ for B and C.
		sortedNodes := [][]netmap.NodeInfo{
			{cnrNodes[0][2], cnrNodes[0][1], cnrNodes[0][0]}, // [C, B, A]
			{cnrNodes[1][0], cnrNodes[1][2], cnrNodes[1][1]}, // [B, D, C]
		}
		m := &metaCollection{
			sortedNodes:   sortedNodes,
			signatures:    make([][]meta.IndexedSignature, 2),
			maxSignatures: []int{1, 2},
		}
		iter := placementIterator{
			log:      zaptest.NewLogger(t),
			neoFSNet: new(testNetwork),
		}
		err := iter.iterateNodesForObject(oidtest.ID(), []uint{1, 2}, cnrNodes, true, func(node nodeDesc) error {
			pk := node.info.PublicKey()
			ind := slices.IndexFunc(m.sortedNodes[node.placementVector], func(info netmap.NodeInfo) bool {
				return bytes.Equal(info.PublicKey(), pk)
			})
			require.GreaterOrEqual(t, ind, 0)
			m.addSignature(node.placementVector, uint8(ind), pk, neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, pk, pk), true)
			return nil
		})
		require.NoError(t, err)

		require.Len(t, m.signatures[0], 1)
		require.Len(t, m.signatures[1], 2)
		for v, sigs := range m.signatures {
			seen := map[uint8]struct{}{}
			for _, sig := range sigs {
				seen[sig.Index] = struct{}{}
				require.Equal(t, m.sortedNodes[v][sig.Index].PublicKey(), sig.Signature.Value())
			}
			require.Len(t, seen, len(sigs))
		}
	})

	t.Run("non-broadcast stays in its vector", func(t *testing.T) {
		cnrNodes := allocNodes([]uint{2, 2})
		cnrNodes[1][0].SetPublicKey(cnrNodes[0][1].PublicKey())

		m := &metaCollection{
			sortedNodes:   cnrNodes,
			signatures:    make([][]meta.IndexedSignature, 2),
			maxSignatures: []int{1, 1},
		}
		pk := cnrNodes[0][1].PublicKey()
		m.addSignature(0, 1, pk, neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, pk, pk), false)
		// Duplicate call for the same node is ignored.
		m.addSignature(0, 1, pk, neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, pk, pk), false)

		require.Len(t, m.signatures[0], 1)
		require.Empty(t, m.signatures[1])
	})
}
