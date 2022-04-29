package pilorama

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

var providers = []struct {
	name      string
	construct func(t *testing.T) Forest
}{
	{"inmemory", func(t *testing.T) Forest {
		f := NewMemoryForest()
		require.NoError(t, f.Init())
		require.NoError(t, f.Open())
		t.Cleanup(func() {
			require.NoError(t, f.Close())
		})

		return f
	}},
	{"bbolt", func(t *testing.T) Forest {
		// Use `os.TempDir` because we construct multiple times in the same test.
		tmpDir, err := os.MkdirTemp(os.TempDir(), "*")
		require.NoError(t, err)

		f := NewBoltForest(filepath.Join(tmpDir, "test.db"))
		require.NoError(t, f.Init())
		require.NoError(t, f.Open())
		t.Cleanup(func() {
			require.NoError(t, f.Close())
			require.NoError(t, os.RemoveAll(tmpDir))
		})
		return f
	}},
}

func testMeta(t *testing.T, f Forest, cid cidSDK.ID, treeID string, nodeID Node, expected Meta) {
	actual, err := f.TreeGetMeta(cid, treeID, nodeID)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
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
	treeID := "version"

	meta := []KeyValue{
		{Key: AttributeVersion, Value: []byte("XXX")},
		{Key: AttributeFilename, Value: []byte("file.txt")}}
	lm, err := s.TreeAddByPath(cid, treeID, AttributeFilename, []string{"path", "to"}, meta)
	require.NoError(t, err)
	require.Equal(t, 3, len(lm))

	nodeID := lm[2].Child
	t.Run("same parent, update meta", func(t *testing.T) {
		_, err = s.TreeMove(cid, treeID, &Move{
			Parent: lm[1].Child,
			Meta:   Meta{Items: append(meta, KeyValue{Key: "NewKey", Value: []byte("NewValue")})},
			Child:  nodeID,
		})
		require.NoError(t, err)

		nodes, err := s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"path", "to", "file.txt"}, false)
		require.NoError(t, err)
		require.ElementsMatch(t, []Node{nodeID}, nodes)
	})
	t.Run("different parent", func(t *testing.T) {
		_, err = s.TreeMove(cid, treeID, &Move{
			Parent: RootID,
			Meta:   Meta{Items: append(meta, KeyValue{Key: "NewKey", Value: []byte("NewValue")})},
			Child:  nodeID,
		})
		require.NoError(t, err)

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
	treeID := "version"

	treeAdd := func(t *testing.T, child, parent Node) {
		_, err := s.TreeMove(cid, treeID, &Move{
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

func TestForest_TreeAdd(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeAdd(t, providers[i].construct(t))
		})
	}
}

func testForestTreeAdd(t *testing.T, s Forest) {
	cid := cidtest.ID()
	treeID := "version"

	meta := []KeyValue{
		{Key: AttributeVersion, Value: []byte("XXX")},
		{Key: AttributeFilename, Value: []byte("file.txt")}}
	lm, err := s.TreeMove(cid, treeID, &Move{
		Parent: RootID,
		Child:  RootID,
		Meta:   Meta{Items: meta},
	})
	require.NoError(t, err)

	testMeta(t, s, cid, treeID, lm.Child, Meta{Time: lm.Time, Items: meta})

	nodes, err := s.TreeGetByPath(cid, treeID, AttributeFilename, []string{"file.txt"}, false)
	require.NoError(t, err)
	require.ElementsMatch(t, []Node{lm.Child}, nodes)

	t.Run("other trees are unaffected", func(t *testing.T) {
		_, err := s.TreeGetByPath(cid, treeID+"123", AttributeFilename, []string{"file.txt"}, false)
		require.ErrorIs(t, err, ErrTreeNotFound)

		_, err = s.TreeGetMeta(cid, treeID+"123", 0)
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
	treeID := "version"

	meta := []KeyValue{
		{Key: AttributeVersion, Value: []byte("XXX")},
		{Key: AttributeFilename, Value: []byte("file.txt")}}
	lm, err := s.TreeAddByPath(cid, treeID, AttributeFilename, []string{"path", "to"}, meta)
	require.NoError(t, err)
	require.Equal(t, 3, len(lm))
	testMeta(t, s, cid, treeID, lm[0].Child, Meta{Time: lm[0].Time, Items: []KeyValue{{AttributeFilename, []byte("path")}}})
	testMeta(t, s, cid, treeID, lm[1].Child, Meta{Time: lm[1].Time, Items: []KeyValue{{AttributeFilename, []byte("to")}}})

	firstID := lm[2].Child
	testMeta(t, s, cid, treeID, firstID, Meta{Time: lm[2].Time, Items: meta})

	meta[0].Value = []byte("YYY")
	lm, err = s.TreeAddByPath(cid, treeID, AttributeFilename, []string{"path", "to"}, meta)
	require.NoError(t, err)
	require.Equal(t, 1, len(lm))

	secondID := lm[0].Child
	testMeta(t, s, cid, treeID, secondID, Meta{Time: lm[0].Time, Items: meta})

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
	lm, err = s.TreeAddByPath(cid, treeID, AttributeFilename, []string{"path", "dir"}, meta)
	require.NoError(t, err)
	require.Equal(t, 2, len(lm))
	testMeta(t, s, cid, treeID, lm[0].Child, Meta{Time: lm[0].Time, Items: []KeyValue{{AttributeFilename, []byte("dir")}}})
	testMeta(t, s, cid, treeID, lm[1].Child, Meta{Time: lm[1].Time, Items: meta})
}

func TestForest_Apply(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeApply(t, providers[i].construct)
		})
	}
}

func testForestTreeApply(t *testing.T, constructor func(t *testing.T) Forest) {
	cid := cidtest.ID()
	treeID := "version"

	testApply := func(t *testing.T, s Forest, child, parent Node, meta Meta) {
		require.NoError(t, s.TreeApply(cid, treeID, &Move{
			Child:  child,
			Parent: parent,
			Meta:   meta,
		}))
	}

	t.Run("add a child, then insert a parent removal", func(t *testing.T) {
		s := constructor(t)
		testApply(t, s, 10, 0, Meta{Time: 1, Items: []KeyValue{{"grand", []byte{1}}}})

		meta := Meta{Time: 3, Items: []KeyValue{{"child", []byte{3}}}}
		testApply(t, s, 11, 10, meta)
		testMeta(t, s, cid, treeID, 11, meta)

		testApply(t, s, 10, TrashID, Meta{Time: 2, Items: []KeyValue{{"parent", []byte{2}}}})
		testMeta(t, s, cid, treeID, 11, Meta{})
	})
	t.Run("add a child to non-existent parent, then add a parent", func(t *testing.T) {
		s := constructor(t)

		testApply(t, s, 11, 10, Meta{Time: 1, Items: []KeyValue{{"child", []byte{3}}}})
		testMeta(t, s, cid, treeID, 11, Meta{})

		testApply(t, s, 10, 0, Meta{Time: 2, Items: []KeyValue{{"grand", []byte{1}}}})
		testMeta(t, s, cid, treeID, 11, Meta{})
	})
}

func TestForest_ApplyRandom(t *testing.T) {
	for i := range providers {
		t.Run(providers[i].name, func(t *testing.T) {
			testForestTreeApplyRandom(t, providers[i].construct)
		})
	}
}

func testForestTreeApplyRandom(t *testing.T, constructor func(t *testing.T) Forest) {
	rand.Seed(42)

	const (
		nodeCount = 4
		opCount   = 10
		iterCount = 100
	)

	cid := cidtest.ID()
	treeID := "version"
	expected := constructor(t)

	ops := make([]Move, nodeCount+opCount)
	for i := 0; i < nodeCount; i++ {
		ops[i] = Move{
			Parent: 0,
			Meta: Meta{
				Time:  Timestamp(i),
				Items: []KeyValue{{Value: make([]byte, 10)}},
			},
			Child: uint64(i) + 1,
		}
		rand.Read(ops[i].Meta.Items[0].Value)
	}

	for i := nodeCount; i < len(ops); i++ {
		ops[i] = Move{
			Parent: rand.Uint64() % (nodeCount + 1),
			Meta: Meta{
				Time:  Timestamp(i + nodeCount),
				Items: []KeyValue{{Value: make([]byte, 10)}},
			},
			Child: rand.Uint64() % (nodeCount + 1),
		}
		rand.Read(ops[i].Meta.Items[0].Value)
	}
	for i := range ops {
		require.NoError(t, expected.TreeApply(cid, treeID, &ops[i]))
	}

	for i := 0; i < iterCount; i++ {
		// Shuffle random operations, leave initialization in place.
		rand.Shuffle(len(ops)-nodeCount, func(i, j int) { ops[i+nodeCount], ops[j+nodeCount] = ops[j+nodeCount], ops[i+nodeCount] })

		actual := constructor(t)
		for i := range ops {
			require.NoError(t, actual.TreeApply(cid, treeID, &ops[i]))
		}
		for i := uint64(0); i < nodeCount; i++ {
			expectedMeta, err := expected.TreeGetMeta(cid, treeID, i)
			require.NoError(t, err)
			actualMeta, err := actual.TreeGetMeta(cid, treeID, i)
			require.NoError(t, err)
			require.Equal(t, expectedMeta, actualMeta, "node id: %d", i)

			if _, ok := actual.(*memoryForest); ok {
				require.Equal(t, expected, actual, i)
			}
		}
	}
}

const benchNodeCount = 1000

func BenchmarkApplySequential(b *testing.B) {
	benchmarkApply(b, benchNodeCount, func(nodeCount, opCount int) []Move {
		ops := make([]Move, opCount)
		for i := range ops {
			ops[i] = Move{
				Parent: uint64(rand.Intn(nodeCount)),
				Meta: Meta{
					Time:  Timestamp(i),
					Items: []KeyValue{{Value: []byte{0, 1, 2, 3, 4}}},
				},
				Child: uint64(rand.Intn(nodeCount)),
			}
		}
		return ops
	})
}

func BenchmarkApplyReorderLast(b *testing.B) {
	// Group operations in a blocks of 10, order blocks in increasing timestamp order,
	// and operations in a single block in reverse.
	const blockSize = 10

	benchmarkApply(b, benchNodeCount, func(nodeCount, opCount int) []Move {
		ops := make([]Move, opCount)
		for i := range ops {
			ops[i] = Move{
				Parent: uint64(rand.Intn(nodeCount)),
				Meta: Meta{
					Time:  Timestamp(i),
					Items: []KeyValue{{Value: []byte{0, 1, 2, 3, 4}}},
				},
				Child: uint64(rand.Intn(nodeCount)),
			}
			if i != 0 && i%blockSize == 0 {
				for j := 0; j < blockSize/2; j++ {
					ops[i-j], ops[i+j-blockSize] = ops[i+j-blockSize], ops[i-j]
				}
			}
		}
		return ops
	})
}

func benchmarkApply(b *testing.B, n int, genFunc func(int, int) []Move) {
	rand.Seed(42)

	s := newState()
	ops := genFunc(n, b.N)

	b.ResetTimer()
	b.ReportAllocs()
	for i := range ops {
		if err := s.Apply(&ops[i]); err != nil {
			b.Fatalf("error in `Apply`: %v", err)
		}
	}
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
	treeID := "version"

	// /
	// |- a (1)
	//    |- cat1.jpg, Version=TTT (3)
	// |- b (2)
	//    |- cat1.jpg, Version=XXX (4)
	//    |- cat1.jpg, Version=YYY (5)
	//    |- cat2.jpg, Version=ZZZ (6)
	testMove(t, s, 0, 1, 0, cid, treeID, "a", "")
	testMove(t, s, 1, 2, 0, cid, treeID, "b", "")
	testMove(t, s, 2, 3, 1, cid, treeID, "cat1.jpg", "TTT")
	testMove(t, s, 3, 4, 2, cid, treeID, "cat1.jpg", "XXX")
	testMove(t, s, 4, 5, 2, cid, treeID, "cat1.jpg", "YYY")
	testMove(t, s, 5, 6, 2, cid, treeID, "cat2.jpg", "ZZZ")

	if mf, ok := s.(*memoryForest); ok {
		single := mf.treeMap[cid.String()+"/"+treeID]
		t.Run("test meta", func(t *testing.T) {
			for i := 0; i < 6; i++ {
				require.Equal(t, uint64(i), single.infoMap[Node(i+1)].Timestamp)
			}
		})
	}

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

func testMove(t *testing.T, s Forest, ts int, node, parent Node, cid cidSDK.ID, treeID, filename, version string) {
	require.NoError(t, s.TreeApply(cid, treeID, &Move{
		Parent: parent,
		Child:  node,
		Meta: Meta{
			Time: uint64(ts),
			Items: []KeyValue{
				{AttributeFilename, []byte(filename)},
				{AttributeVersion, []byte(version)},
			},
		},
	}))
}
