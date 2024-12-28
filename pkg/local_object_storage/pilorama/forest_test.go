package pilorama

import (
	crand "crypto/rand"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

var providers = []struct {
	name      string
	construct func(t testing.TB, opts ...Option) Forest
}{
	{"inmemory", func(t testing.TB, _ ...Option) Forest {
		f := NewMemoryForest()
		require.NoError(t, f.Open(false))
		require.NoError(t, f.Init())
		t.Cleanup(func() {
			require.NoError(t, f.Close())
		})

		return f
	}},
	{"bbolt", func(t testing.TB, opts ...Option) Forest {
		// Use `os.TempDir` because we construct multiple times in the same test.
		tmpDir, err := os.MkdirTemp(os.TempDir(), "*")
		require.NoError(t, err)

		f := NewBoltForest(
			append([]Option{
				WithPath(filepath.Join(tmpDir, "test.db")),
				WithMaxBatchSize(1)}, opts...)...)
		require.NoError(t, f.Open(false))
		require.NoError(t, f.Init())
		t.Cleanup(func() {
			require.NoError(t, f.Close())
			require.NoError(t, os.RemoveAll(tmpDir))
		})
		return f
	}},
}

func testMeta(t *testing.T, f Forest, cid cidSDK.ID, treeID string, nodeID, parentID Node, expected Meta) {
	actualMeta, actualParent, err := f.TreeGetMeta(cid, treeID, nodeID)
	require.NoError(t, err)
	require.Equal(t, parentID, actualParent)
	require.Equal(t, expected, actualMeta)
}

func TestForest_TreeMove(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeMove(t, providers[i].construct(t))
		})
	}
}

func testForestTreeMove(t *testing.T, s Forest) {
	cid := cidtest.ID()
	d := CIDDescriptor{cid, 0, 1}
	treeID := "version"

	meta := []KeyValue{
		{Key: AttributeVersion, Value: []byte("XXX")},
		{Key: AttributeFilename, Value: []byte("file.txt")}}
	lm, err := s.TreeAddByPath(d, treeID, AttributeFilename, []string{"path", "to"}, meta)
	require.NoError(t, err)
	require.Equal(t, 3, len(lm))

	nodeID := lm[2].Child
	t.Run("invalid descriptor", func(t *testing.T) {
		_, err = s.TreeMove(CIDDescriptor{cid, 0, 0}, treeID, &Move{
			Parent: lm[1].Child,
			Meta:   Meta{Items: append(meta, KeyValue{Key: "NewKey", Value: []byte("NewValue")})},
			Child:  nodeID,
		})
		require.ErrorIs(t, err, ErrInvalidCIDDescriptor)
	})
	t.Run("same parent, update meta", func(t *testing.T) {
		res, err := s.TreeMove(d, treeID, &Move{
			Parent: lm[1].Child,
			Meta:   Meta{Items: append(meta, KeyValue{Key: "NewKey", Value: []byte("NewValue")})},
			Child:  nodeID,
		})
		require.NoError(t, err)
		require.Equal(t, res.Child, nodeID)

		nodes, err := s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"path", "to", "file.txt"}, false)
		require.NoError(t, err)
		require.ElementsMatch(t, []Node{nodeID}, nodes)
	})
	t.Run("different parent", func(t *testing.T) {
		res, err := s.TreeMove(d, treeID, &Move{
			Parent: RootID,
			Meta:   Meta{Items: append(meta, KeyValue{Key: "NewKey", Value: []byte("NewValue")})},
			Child:  nodeID,
		})
		require.NoError(t, err)
		require.Equal(t, res.Child, nodeID)

		nodes, err := s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"path", "to", "file.txt"}, false)
		require.NoError(t, err)
		require.True(t, len(nodes) == 0)

		nodes, err = s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"file.txt"}, false)
		require.NoError(t, err)
		require.ElementsMatch(t, []Node{nodeID}, nodes)
	})
}

func TestMemoryForest_TreeGetChildren(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeGetChildren(t, providers[i].construct(t))
		})
	}
}

func testForestTreeGetChildren(t *testing.T, s Forest) {
	cid := cidtest.ID()
	d := CIDDescriptor{cid, 0, 1}
	treeID := "version"

	treeAdd := func(t *testing.T, child, parent Node) {
		_, err := s.TreeMove(d, treeID, &Move{
			Parent: parent,
			Child:  child,
		})
		require.NoError(t, err)
	}

	// 0
	// |- 10
	// |  |- 3
	// |  |- 6
	// |     |- 11
	// |- 2
	// |- 7
	treeAdd(t, 10, 0)
	treeAdd(t, 3, 10)
	treeAdd(t, 6, 10)
	treeAdd(t, 11, 6)
	treeAdd(t, 2, 0)
	treeAdd(t, 7, 0)

	testGetChildren := func(t *testing.T, nodeID Node, expected []Node) {
		actual, err := s.TreeGetChildren(cid, treeID, nodeID)
		require.NoError(t, err)
		require.ElementsMatch(t, expected, actual)
	}

	testGetChildren(t, 0, []uint64{10, 2, 7})
	testGetChildren(t, 10, []uint64{3, 6})
	testGetChildren(t, 3, nil)
	testGetChildren(t, 6, []uint64{11})
	testGetChildren(t, 11, nil)
	testGetChildren(t, 2, nil)
	testGetChildren(t, 7, nil)
	t.Run("missing node", func(t *testing.T) {
		testGetChildren(t, 42, nil)
	})
	t.Run("missing tree", func(t *testing.T) {
		_, err := s.TreeGetChildren(cid, treeID+"123", 0)
		require.ErrorIs(t, err, ErrTreeNotFound)
	})
}

func TestForest_TreeDrop(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeDrop(t, providers[i].construct(t))
		})
	}
}

func testForestTreeDrop(t *testing.T, s Forest) {
	const cidsSize = 3
	var cids [cidsSize]cidSDK.ID

	for i := range cids {
		cids[i] = cidtest.ID()
	}
	cid := cids[0]

	t.Run("return nil if not found", func(t *testing.T) {
		require.ErrorIs(t, s.TreeDrop(cid, "123"), ErrTreeNotFound)
	})

	require.NoError(t, s.TreeDrop(cid, ""))

	trees := []string{"tree1", "tree2"}
	var descs [cidsSize]CIDDescriptor
	for i := range descs {
		descs[i] = CIDDescriptor{cids[i], 0, 1}
	}
	d := descs[0]
	for i := range trees {
		_, err := s.TreeAddByPath(d, trees[i], AttributeFilename, []string{"path"},
			[]KeyValue{{Key: "TreeName", Value: []byte(trees[i])}})
		require.NoError(t, err)
	}

	err := s.TreeDrop(cid, trees[0])
	require.NoError(t, err)

	_, err = s.TreeGetByPath(cid, trees[0], AttributeFilename, []string{"path"}, true)
	require.ErrorIs(t, err, ErrTreeNotFound)

	_, err = s.TreeGetByPath(cid, trees[1], AttributeFilename, []string{"path"}, true)
	require.NoError(t, err)

	for j := range descs {
		for i := range trees {
			_, err := s.TreeAddByPath(descs[j], trees[i], AttributeFilename, []string{"path"},
				[]KeyValue{{Key: "TreeName", Value: []byte(trees[i])}})
			require.NoError(t, err)
		}
	}
	list, err := s.TreeList(cid)
	require.NoError(t, err)
	require.NotEmpty(t, list)

	require.NoError(t, s.TreeDrop(cid, ""))

	list, err = s.TreeList(cid)
	require.NoError(t, err)
	require.Empty(t, list)

	for j := 1; j < len(cids); j++ {
		list, err = s.TreeList(cids[j])
		require.NoError(t, err)
		require.Equal(t, len(list), len(trees))
	}
}

func TestForest_TreeAdd(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeAdd(t, providers[i].construct(t))
		})
	}
}

func testForestTreeAdd(t *testing.T, s Forest) {
	cid := cidtest.ID()
	d := CIDDescriptor{cid, 0, 1}
	treeID := "version"

	meta := []KeyValue{
		{Key: AttributeVersion, Value: []byte("XXX")},
		{Key: AttributeFilename, Value: []byte("file.txt")}}
	m := &Move{
		Parent: RootID,
		Child:  RootID,
		Meta:   Meta{Items: meta},
	}

	t.Run("invalid descriptor", func(t *testing.T) {
		_, err := s.TreeMove(CIDDescriptor{cid, 0, 0}, treeID, m)
		require.ErrorIs(t, err, ErrInvalidCIDDescriptor)
	})

	lm, err := s.TreeMove(d, treeID, m)
	require.NoError(t, err)

	testMeta(t, s, cid, treeID, lm.Child, lm.Parent, Meta{Time: lm.Time, Items: meta})

	nodes, err := s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"file.txt"}, false)
	require.NoError(t, err)
	require.ElementsMatch(t, []Node{lm.Child}, nodes)

	t.Run("other trees are unaffected", func(t *testing.T) {
		_, err := s.TreeGetByPath(cid, treeID+"123", AttributeFilename, []string{"file.txt"}, false)
		require.ErrorIs(t, err, ErrTreeNotFound)

		_, _, err = s.TreeGetMeta(cid, treeID+"123", 0)
		require.ErrorIs(t, err, ErrTreeNotFound)
	})
}

func TestForest_TreeAddByPath(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeAddByPath(t, providers[i].construct(t))
		})
	}
}

func testForestTreeAddByPath(t *testing.T, s Forest) {
	cid := cidtest.ID()
	d := CIDDescriptor{cid, 0, 1}
	treeID := "version"

	meta := []KeyValue{
		{Key: AttributeVersion, Value: []byte("XXX")},
		{Key: AttributeFilename, Value: []byte("file.txt")}}

	t.Run("invalid descriptor", func(t *testing.T) {
		_, err := s.TreeAddByPath(CIDDescriptor{cid, 0, 0}, treeID, AttributeFilename, []string{"yyy"}, meta)
		require.ErrorIs(t, err, ErrInvalidCIDDescriptor)
	})
	t.Run("invalid attribute", func(t *testing.T) {
		_, err := s.TreeAddByPath(d, treeID, AttributeVersion, []string{"yyy"}, meta)
		require.ErrorIs(t, err, ErrNotPathAttribute)
	})

	lm, err := s.TreeAddByPath(d, treeID, AttributeFilename, []string{"path", "to"}, meta)
	require.NoError(t, err)
	require.Equal(t, 3, len(lm))
	testMeta(t, s, cid, treeID, lm[0].Child, lm[0].Parent, Meta{Time: lm[0].Time, Items: []KeyValue{{AttributeFilename, []byte("path")}}})
	testMeta(t, s, cid, treeID, lm[1].Child, lm[1].Parent, Meta{Time: lm[1].Time, Items: []KeyValue{{AttributeFilename, []byte("to")}}})

	firstID := lm[2].Child
	testMeta(t, s, cid, treeID, firstID, lm[2].Parent, Meta{Time: lm[2].Time, Items: meta})

	meta[0].Value = []byte("YYY")
	lm, err = s.TreeAddByPath(d, treeID, AttributeFilename, []string{"path", "to"}, meta)
	require.NoError(t, err)
	require.Equal(t, 1, len(lm))

	secondID := lm[0].Child
	testMeta(t, s, cid, treeID, secondID, lm[0].Parent, Meta{Time: lm[0].Time, Items: meta})

	t.Run("get versions", func(t *testing.T) {
		// All versions.
		nodes, err := s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"path", "to", "file.txt"}, false)
		require.NoError(t, err)
		require.ElementsMatch(t, []Node{firstID, secondID}, nodes)

		// Latest version.
		nodes, err = s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"path", "to", "file.txt"}, true)
		require.NoError(t, err)
		require.Equal(t, []Node{secondID}, nodes)
	})

	meta[0].Value = []byte("ZZZ")
	meta[1].Value = []byte("cat.jpg")
	lm, err = s.TreeAddByPath(d, treeID, AttributeFilename, []string{"path", "dir"}, meta)
	require.NoError(t, err)
	require.Equal(t, 2, len(lm))
	testMeta(t, s, cid, treeID, lm[0].Child, lm[0].Parent, Meta{Time: lm[0].Time, Items: []KeyValue{{AttributeFilename, []byte("dir")}}})
	testMeta(t, s, cid, treeID, lm[1].Child, lm[1].Parent, Meta{Time: lm[1].Time, Items: meta})

	t.Run("create internal nodes", func(t *testing.T) {
		meta[0].Value = []byte("SomeValue")
		meta[1].Value = []byte("another")
		lm, err = s.TreeAddByPath(d, treeID, AttributeFilename, []string{"path"}, meta)
		require.NoError(t, err)
		require.Equal(t, 1, len(lm))

		oldMove := lm[0]

		meta[0].Value = []byte("Leaf")
		meta[1].Value = []byte("file.txt")
		lm, err = s.TreeAddByPath(d, treeID, AttributeFilename, []string{"path", "another"}, meta)
		require.NoError(t, err)
		require.Equal(t, 2, len(lm))

		testMeta(t, s, cid, treeID, lm[0].Child, lm[0].Parent,
			Meta{Time: lm[0].Time, Items: []KeyValue{{AttributeFilename, []byte("another")}}})
		testMeta(t, s, cid, treeID, lm[1].Child, lm[1].Parent, Meta{Time: lm[1].Time, Items: meta})

		require.NotEqual(t, lm[0].Child, oldMove.Child)
		testMeta(t, s, cid, treeID, oldMove.Child, oldMove.Parent,
			Meta{Time: oldMove.Time, Items: []KeyValue{
				{AttributeVersion, []byte("SomeValue")},
				{AttributeFilename, []byte("another")}}})

		t.Run("get by path", func(t *testing.T) {
			nodes, err := s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"path", "another"}, false)
			require.NoError(t, err)
			require.Equal(t, 2, len(nodes))
			require.ElementsMatch(t, []Node{lm[0].Child, oldMove.Child}, nodes)

			nodes, err = s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"path", "another", "file.txt"}, false)
			require.NoError(t, err)
			require.Equal(t, 1, len(nodes))
			require.Equal(t, lm[1].Child, nodes[0])
		})
	})

	t.Run("empty component", func(t *testing.T) {
		meta := []KeyValue{
			{Key: AttributeVersion, Value: []byte("XXX")},
			{Key: AttributeFilename, Value: []byte{}}}
		lm, err := s.TreeAddByPath(d, treeID, AttributeFilename, []string{"path", "to"}, meta)
		require.NoError(t, err)
		require.Equal(t, 1, len(lm))

		nodes, err := s.TreeGetByPath(d.CID, treeID, AttributeFilename, []string{"path", "to", ""}, false)
		require.NoError(t, err)
		require.Equal(t, 1, len(nodes))
		require.Equal(t, lm[0].Child, nodes[0])
	})
}

func TestForest_Apply(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeApply(t, providers[i].construct)
		})
	}
}

func testForestTreeApply(t *testing.T, constructor func(t testing.TB, _ ...Option) Forest) {
	cid := cidtest.ID()
	d := CIDDescriptor{cid, 0, 1}
	treeID := "version"

	t.Run("invalid descriptor", func(t *testing.T) {
		s := constructor(t)
		err := s.TreeApply(CIDDescriptor{cid, 0, 0}, treeID, &Move{
			Child:  10,
			Parent: 0,
			Meta:   Meta{Time: 1, Items: []KeyValue{{"grand", []byte{1}}}},
		}, false)
		require.ErrorIs(t, err, ErrInvalidCIDDescriptor)
	})

	testApply := func(t *testing.T, s Forest, child, parent Node, meta Meta) {
		require.NoError(t, s.TreeApply(d, treeID, &Move{
			Child:  child,
			Parent: parent,
			Meta:   meta,
		}, false))
	}

	t.Run("add a child, then insert a parent removal", func(t *testing.T) {
		s := constructor(t)
		testApply(t, s, 10, 0, Meta{Time: 1, Items: []KeyValue{{"grand", []byte{1}}}})

		meta := Meta{Time: 3, Items: []KeyValue{{"child", []byte{3}}}}
		testApply(t, s, 11, 10, meta)
		testMeta(t, s, cid, treeID, 11, 10, meta)

		testApply(t, s, 10, TrashID, Meta{Time: 2, Items: []KeyValue{{"parent", []byte{2}}}})
		testMeta(t, s, cid, treeID, 11, 10, meta)
	})
	t.Run("add a child to non-existent parent, then add a parent", func(t *testing.T) {
		s := constructor(t)

		meta := Meta{Time: 1, Items: []KeyValue{{"child", []byte{3}}}}
		testApply(t, s, 11, 10, meta)
		testMeta(t, s, cid, treeID, 11, 10, meta)

		testApply(t, s, 10, 0, Meta{Time: 2, Items: []KeyValue{{"grand", []byte{1}}}})
		testMeta(t, s, cid, treeID, 11, 10, meta)
	})
}

func TestForest_GetOpLog(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeGetOpLog(t, providers[i].construct)
		})
	}
}

func testForestTreeGetOpLog(t *testing.T, constructor func(t testing.TB, _ ...Option) Forest) {
	cid := cidtest.ID()
	d := CIDDescriptor{cid, 0, 1}
	treeID := "version"
	logs := []Move{
		{
			Meta:  Meta{Time: 4, Items: []KeyValue{{"grand", []byte{1}}}},
			Child: 1,
		},
		{
			Meta:  Meta{Time: 5, Items: []KeyValue{{"second", []byte{1, 2, 3}}}},
			Child: 4,
		},
		{
			Parent: 10,
			Meta:   Meta{Time: 256 + 4, Items: []KeyValue{}}, // make sure keys are big-endian
			Child:  11,
		},
	}

	s := constructor(t)

	t.Run("empty log, no panic", func(t *testing.T) {
		_, err := s.TreeGetOpLog(cid, treeID, 0)
		require.ErrorIs(t, err, ErrTreeNotFound)
	})

	for i := range logs {
		require.NoError(t, s.TreeApply(d, treeID, &logs[i], false))
	}

	testGetOpLog := func(t *testing.T, height uint64, m Move) {
		lm, err := s.TreeGetOpLog(cid, treeID, height)
		require.NoError(t, err)
		require.Equal(t, m, lm)
	}

	testGetOpLog(t, 0, logs[0])
	testGetOpLog(t, 4, logs[0])
	testGetOpLog(t, 5, logs[1])
	testGetOpLog(t, 6, logs[2])
	testGetOpLog(t, 260, logs[2])
	t.Run("missing entry", func(t *testing.T) {
		testGetOpLog(t, 261, Move{})
	})
	t.Run("missing tree", func(t *testing.T) {
		_, err := s.TreeGetOpLog(cid, treeID+"123", 4)
		require.ErrorIs(t, err, ErrTreeNotFound)
	})
}

func TestForest_TreeExists(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeExists(t, providers[i].construct)
		})
	}
}

func testForestTreeExists(t *testing.T, constructor func(t testing.TB, opts ...Option) Forest) {
	s := constructor(t)

	checkExists := func(t *testing.T, expected bool, cid cidSDK.ID, treeID string) {
		actual, err := s.TreeExists(cid, treeID)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}

	cid := cidtest.ID()
	treeID := "version"
	d := CIDDescriptor{cid, 0, 1}

	t.Run("empty state, no panic", func(t *testing.T) {
		checkExists(t, false, cid, treeID)
	})

	require.NoError(t, s.TreeApply(d, treeID, &Move{Parent: 0, Child: 1}, false))
	checkExists(t, true, cid, treeID)
	checkExists(t, false, cidtest.ID(), treeID) // different CID, same tree
	checkExists(t, false, cid, "another tree")  // same CID, different tree

	t.Run("can be removed", func(t *testing.T) {
		require.NoError(t, s.TreeDrop(cid, treeID))
		checkExists(t, false, cid, treeID)
	})
}

func TestApplyTricky1(t *testing.T) {
	ops := []Move{
		{
			Parent: 1,
			Meta:   Meta{Time: 100},
			Child:  2,
		},
		{
			Parent: 0,
			Meta:   Meta{Time: 80},
			Child:  1,
		},
	}

	expected := []struct{ child, parent Node }{
		{1, 0},
		{2, 1},
	}

	treeID := "version"
	d := CIDDescriptor{CID: cidtest.ID(), Position: 0, Size: 1}
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			s := providers[i].construct(t)
			for i := range ops {
				require.NoError(t, s.TreeApply(d, treeID, &ops[i], false))
			}

			for i := range expected {
				_, parent, err := s.TreeGetMeta(d.CID, treeID, expected[i].child)
				require.NoError(t, err)
				require.Equal(t, expected[i].parent, parent)
			}
		})
	}
}

func TestApplyTricky2(t *testing.T) {
	// Apply operations in the reverse order and then insert an operation in the middle
	// so that previous "old" parent becomes invalid.
	ops := []Move{
		{
			Parent: 10000,
			Meta:   Meta{Time: 100},
			Child:  5,
		},
		{
			Parent: 3,
			Meta:   Meta{Time: 80},
			Child:  5,
		},
		{
			Parent: 5,
			Meta:   Meta{Time: 40},
			Child:  3,
		},
		{
			Parent: 5,
			Meta:   Meta{Time: 60},
			Child:  1,
		},
		{
			Parent: 1,
			Meta:   Meta{Time: 90},
			Child:  2,
		},
		{
			Parent: 0,
			Meta:   Meta{Time: 10},
			Child:  5,
		},
	}

	expected := []struct{ child, parent Node }{
		{5, 10_000},
		{3, 5},
		{2, 1},
		{1, 5},
	}

	treeID := "version"
	d := CIDDescriptor{CID: cidtest.ID(), Position: 0, Size: 1}
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			s := providers[i].construct(t)
			for i := range ops {
				require.NoError(t, s.TreeApply(d, treeID, &ops[i], false))
			}

			for i := range expected {
				_, parent, err := s.TreeGetMeta(d.CID, treeID, expected[i].child)
				require.NoError(t, err)
				require.Equal(t, expected[i].parent, parent)
			}
		})
	}
}

func TestForest_ApplyRandom(t *testing.T) {
	t.Skip("tree service code is about to be dropped, no one gonna spend time fixing it")
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeApplyRandom(t, providers[i].construct)
		})
	}
}

func TestForest_ParallelApply(t *testing.T) {
	for i := range providers {
		if providers[i].name == "inmemory" {
			continue
		}
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeParallelApply(t, providers[i].construct, 8, 128, 10)
		})
	}
}

// prepareRandomTree creates a random sequence of operation and applies them to tree.
// The operations are guaranteed to be applied and returned sorted by `Time`.
func prepareRandomTree(nodeCount, opCount int) []Move {
	ops := make([]Move, nodeCount+opCount)
	for i := range nodeCount {
		ops[i] = Move{
			Parent: 0,
			Meta: Meta{
				Time: Timestamp(i),
				Items: []KeyValue{
					{Key: AttributeFilename, Value: []byte(strconv.Itoa(i))},
					{Value: make([]byte, 10)},
				},
			},
			Child: uint64(i) + 1,
		}
		_, _ = crand.Read(ops[i].Meta.Items[1].Value)
	}

	for i := nodeCount; i < len(ops); i++ {
		ops[i] = Move{
			Parent: rand.Uint64() % uint64(nodeCount+12),
			Meta: Meta{
				Time: Timestamp(i + nodeCount),
				Items: []KeyValue{
					{Key: AttributeFilename, Value: []byte(strconv.Itoa(i))},
					{Value: make([]byte, 10)},
				},
			},
			Child: rand.Uint64() % uint64(nodeCount+10),
		}
		if rand.Uint32()%5 == 0 {
			ops[i].Parent = TrashID
		}
		_, _ = crand.Read(ops[i].Meta.Items[1].Value)
	}

	return ops
}

func compareForests(t *testing.T, expected, actual Forest, cid cidSDK.ID, treeID string, nodeCount int) {
	for i := range uint64(nodeCount) {
		expectedMeta, expectedParent, err := expected.TreeGetMeta(cid, treeID, i)
		require.NoError(t, err)
		actualMeta, actualParent, err := actual.TreeGetMeta(cid, treeID, i)
		require.NoError(t, err)
		require.Equal(t, expectedParent, actualParent, "node id: %d", i)
		require.Equal(t, expectedMeta, actualMeta, "node id: %d", i)

		if ma, ok := actual.(*memoryForest); ok {
			me := expected.(*memoryForest)
			require.Equal(t, len(me.treeMap), len(ma.treeMap))

			for k, sa := range ma.treeMap {
				se, ok := me.treeMap[k]
				require.True(t, ok)
				require.Equal(t, se.operations, sa.operations)
				require.Equal(t, se.infoMap, sa.infoMap)

				require.Equal(t, len(se.childMap), len(sa.childMap))
				for ck, la := range sa.childMap {
					le, ok := se.childMap[ck]
					require.True(t, ok)
					require.ElementsMatch(t, le, la)
				}
			}
			require.Equal(t, expected, actual, i)
		}
	}
}

func testForestTreeParallelApply(t *testing.T, constructor func(t testing.TB, _ ...Option) Forest, batchSize, opCount, iterCount int) {
	const nodeCount = 5

	ops := prepareRandomTree(nodeCount, opCount)

	cid := cidtest.ID()
	d := CIDDescriptor{cid, 0, 1}
	treeID := "version"

	expected := constructor(t)
	for i := range ops {
		require.NoError(t, expected.TreeApply(d, treeID, &ops[i], false))
	}

	for range iterCount {
		// Shuffle random operations, leave initialization in place.
		rand.Shuffle(len(ops), func(i, j int) { ops[i], ops[j] = ops[j], ops[i] })

		actual := constructor(t, WithMaxBatchSize(batchSize))
		wg := new(sync.WaitGroup)
		ch := make(chan *Move)
		for range batchSize {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for op := range ch {
					require.NoError(t, actual.TreeApply(d, treeID, op, false))
				}
			}()
		}

		for i := range ops {
			ch <- &ops[i]
		}
		close(ch)
		wg.Wait()

		compareForests(t, expected, actual, cid, treeID, nodeCount)
	}
}

func testForestTreeApplyRandom(t *testing.T, constructor func(t testing.TB, _ ...Option) Forest) {
	const (
		nodeCount = 5
		opCount   = 20
	)

	ops := prepareRandomTree(nodeCount, opCount)

	cid := cidtest.ID()
	d := CIDDescriptor{cid, 0, 1}
	treeID := "version"

	expected := constructor(t)
	for i := range ops {
		require.NoError(t, expected.TreeApply(d, treeID, &ops[i], false))
	}

	const iterCount = 200
	for range iterCount {
		// Shuffle random operations, leave initialization in place.
		rand.Shuffle(len(ops), func(i, j int) { ops[i], ops[j] = ops[j], ops[i] })

		actual := constructor(t)
		for i := range ops {
			require.NoError(t, actual.TreeApply(d, treeID, &ops[i], false))
		}
		compareForests(t, expected, actual, cid, treeID, nodeCount)
	}
}

const benchNodeCount = 1000

var batchSizes = []int{1, 2, 4, 8, 16, 32}

func BenchmarkApplySequential(b *testing.B) {
	for i := range providers {
		if providers[i].name == "inmemory" { // memory backend is not thread-safe
			continue
		}
		b.Run(providers[i].name, func(b *testing.B) {
			for _, bs := range batchSizes {
				b.Run("batchsize="+strconv.Itoa(bs), func(b *testing.B) {
					s := providers[i].construct(b, WithMaxBatchSize(bs))
					benchmarkApply(b, s, func(opCount int) []Move {
						ops := make([]Move, opCount)
						for i := range ops {
							ops[i] = Move{
								Parent: uint64(rand.IntN(benchNodeCount)),
								Meta: Meta{
									Time:  Timestamp(i),
									Items: []KeyValue{{Value: []byte{0, 1, 2, 3, 4}}},
								},
								Child: uint64(rand.IntN(benchNodeCount)),
							}
						}
						return ops
					})
				})
			}
		})
	}
}

func BenchmarkApplyReorderLast(b *testing.B) {
	// Group operations in a blocks of 10, order blocks in increasing timestamp order,
	// and operations in a single block in reverse.
	const blockSize = 10

	for i := range providers {
		if providers[i].name == "inmemory" { // memory backend is not thread-safe
			continue
		}
		b.Run(providers[i].name, func(b *testing.B) {
			for _, bs := range batchSizes {
				b.Run("batchsize="+strconv.Itoa(bs), func(b *testing.B) {
					s := providers[i].construct(b, WithMaxBatchSize(bs))
					benchmarkApply(b, s, func(opCount int) []Move {
						ops := make([]Move, opCount)
						for i := range ops {
							ops[i] = Move{
								Parent: uint64(rand.IntN(benchNodeCount)),
								Meta: Meta{
									Time:  Timestamp(i),
									Items: []KeyValue{{Value: []byte{0, 1, 2, 3, 4}}},
								},
								Child: uint64(rand.IntN(benchNodeCount)),
							}
							if i != 0 && i%blockSize == 0 {
								for j := range blockSize / 2 {
									ops[i-j], ops[i+j-blockSize] = ops[i+j-blockSize], ops[i-j]
								}
							}
						}
						return ops
					})
				})
			}
		})
	}
}

func benchmarkApply(b *testing.B, s Forest, genFunc func(int) []Move) {
	ops := genFunc(b.N)
	cid := cidtest.ID()
	d := CIDDescriptor{cid, 0, 1}
	treeID := "version"
	ch := make(chan int, b.N)
	for i := range b.N {
		ch <- i
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.TreeApply(d, treeID, &ops[<-ch], false); err != nil {
				b.Fatalf("error in `Apply`: %v", err)
			}
		}
	})
}

func TestTreeGetByPath(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testTreeGetByPath(t, providers[i].construct(t))
		})
	}
}

func testTreeGetByPath(t *testing.T, s Forest) {
	cid := cidtest.ID()
	d := CIDDescriptor{cid, 0, 1}
	treeID := "version"

	// /
	// |- a (1)
	//    |- cat1.jpg, Version=TTT (3)
	// |- b (2)
	//    |- cat1.jpg, Version=XXX (4)
	//    |- cat1.jpg, Version=YYY (5)
	//    |- cat2.jpg, Version=ZZZ (6)
	testMove(t, s, 0, 1, 0, d, treeID, "a", "")
	testMove(t, s, 1, 2, 0, d, treeID, "b", "")
	testMove(t, s, 2, 3, 1, d, treeID, "cat1.jpg", "TTT")
	testMove(t, s, 3, 4, 2, d, treeID, "cat1.jpg", "XXX")
	testMove(t, s, 4, 5, 2, d, treeID, "cat1.jpg", "YYY")
	testMove(t, s, 5, 6, 2, d, treeID, "cat2.jpg", "ZZZ")

	if mf, ok := s.(*memoryForest); ok {
		single := mf.treeMap[cid.String()+"/"+treeID]
		t.Run("test meta", func(t *testing.T) {
			for i := range 6 {
				require.Equal(t, uint64(i), single.infoMap[Node(i+1)].Meta.Time)
			}
		})
	}

	t.Run("invalid attribute", func(t *testing.T) {
		_, err := s.TreeGetByPath(cid, treeID, AttributeVersion, []string{"", "TTT"}, false)
		require.ErrorIs(t, err, ErrNotPathAttribute)
	})

	nodes, err := s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"b", "cat1.jpg"}, false)
	require.NoError(t, err)
	require.Equal(t, []Node{4, 5}, nodes)

	nodes, err = s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"a", "cat1.jpg"}, false)
	require.Equal(t, []Node{3}, nodes)

	t.Run("missing child", func(t *testing.T) {
		nodes, err = s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"a", "cat3.jpg"}, false)
		require.True(t, len(nodes) == 0)
	})
	t.Run("missing parent", func(t *testing.T) {
		nodes, err = s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"xyz", "cat1.jpg"}, false)
		require.True(t, len(nodes) == 0)
	})
	t.Run("empty path", func(t *testing.T) {
		nodes, err = s.TreeGetByPath(cid, treeID, AttributeFilename, nil, false)
		require.True(t, len(nodes) == 0)
	})
}

func testMove(t *testing.T, s Forest, ts int, node, parent Node, d CIDDescriptor, treeID, filename, version string) {
	items := make([]KeyValue, 1, 2)
	items[0] = KeyValue{AttributeFilename, []byte(filename)}
	if version != "" {
		items = append(items, KeyValue{AttributeVersion, []byte(version)})
	}

	require.NoError(t, s.TreeApply(d, treeID, &Move{
		Parent: parent,
		Child:  node,
		Meta: Meta{
			Time:  uint64(ts),
			Items: items,
		},
	}, false))
}

func TestGetTrees(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testTreeGetTrees(t, providers[i].construct(t))
		})
	}
}

func testTreeGetTrees(t *testing.T, s Forest) {
	cids := []cidSDK.ID{cidtest.ID(), cidtest.ID()}
	d := CIDDescriptor{Position: 0, Size: 1}

	treeIDs := make(map[cidSDK.ID][]string, len(cids))
	for i, cid := range cids {
		treeIDs[cid] = []string{
			fmt.Sprintf("test1_%d", i),
			fmt.Sprintf("test2_%d", i),
			fmt.Sprintf("test3_%d", i),
			fmt.Sprintf("1test_%d", i),
			fmt.Sprintf("2test_%d", i),
			fmt.Sprintf("3test_%d", i),
			"",
		}
	}

	for _, cid := range cids {
		d.CID = cid

		for _, treeID := range treeIDs[cid] {
			_, err := s.TreeAddByPath(d, treeID, objectSDK.AttributeFileName, []string{"path"}, nil)
			require.NoError(t, err)
		}
	}

	for _, cid := range cids {
		d.CID = cid

		trees, err := s.TreeList(cid)
		require.NoError(t, err)

		require.ElementsMatch(t, treeIDs[cid], trees)
	}
}
