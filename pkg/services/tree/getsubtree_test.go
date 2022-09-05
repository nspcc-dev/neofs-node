package tree

import (
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestGetSubTree(t *testing.T) {
	d := pilorama.CIDDescriptor{CID: cidtest.ID(), Size: 1}
	treeID := "sometree"
	p := pilorama.NewMemoryForest()

	tree := []struct {
		path []string
		id   uint64
	}{
		{path: []string{"dir1"}},
		{path: []string{"dir2"}},
		{path: []string{"dir1", "sub1"}},
		{path: []string{"dir2", "sub1"}},
		{path: []string{"dir2", "sub2"}},
		{path: []string{"dir2", "sub1", "subsub1"}},
	}

	for i := range tree {
		path := tree[i].path
		meta := []pilorama.KeyValue{
			{Key: pilorama.AttributeFilename, Value: []byte(path[len(path)-1])}}

		lm, err := p.TreeAddByPath(d, treeID, pilorama.AttributeFilename, path[:len(path)-1], meta)
		require.NoError(t, err)
		require.Equal(t, 1, len(lm))

		tree[i].id = lm[0].Child
	}

	testGetSubTree := func(t *testing.T, rootID uint64, depth uint32, errIndex int) []uint64 {
		acc := subTreeAcc{errIndex: errIndex}
		err := getSubTree(&acc, d.CID, &GetSubTreeRequest_Body{
			TreeId: treeID,
			RootId: rootID,
			Depth:  depth,
		}, p)
		if errIndex == -1 {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, errSubTreeSend)
		}

		// GetSubTree must return child only after is has returned the parent.
		require.Equal(t, rootID, acc.seen[0].Body.NodeId)
	loop:
		for i := 1; i < len(acc.seen); i++ {
			parent := acc.seen[i].Body.ParentId
			for j := 0; j < i; j++ {
				if acc.seen[j].Body.NodeId == parent {
					continue loop
				}
			}
			require.Fail(t, "node has parent %d, but it hasn't been seen", parent)
		}

		// GetSubTree must return valid meta.
		for i := range acc.seen {
			b := acc.seen[i].Body
			meta, node, err := p.TreeGetMeta(d.CID, treeID, b.NodeId)
			require.NoError(t, err)
			require.Equal(t, node, b.ParentId)
			require.Equal(t, meta.Time, b.Timestamp)
			require.Equal(t, metaToProto(meta.Items), b.Meta)
		}

		ordered := make([]uint64, len(acc.seen))
		for i := range acc.seen {
			ordered[i] = acc.seen[i].Body.NodeId
		}
		return ordered
	}

	t.Run("depth = 1, only root", func(t *testing.T) {
		actual := testGetSubTree(t, 0, 1, -1)
		require.Equal(t, []uint64{0}, actual)

		t.Run("custom root", func(t *testing.T) {
			actual := testGetSubTree(t, tree[2].id, 1, -1)
			require.Equal(t, []uint64{tree[2].id}, actual)
		})
	})
	t.Run("depth = 2", func(t *testing.T) {
		actual := testGetSubTree(t, 0, 2, -1)
		require.Equal(t, []uint64{0, tree[0].id, tree[1].id}, actual)

		t.Run("error in the middle", func(t *testing.T) {
			actual := testGetSubTree(t, 0, 2, 0)
			require.Equal(t, []uint64{0}, actual)

			actual = testGetSubTree(t, 0, 2, 1)
			require.Equal(t, []uint64{0, tree[0].id}, actual)
		})
	})
	t.Run("depth = 0 (unrestricted)", func(t *testing.T) {
		actual := testGetSubTree(t, 0, 0, -1)
		expected := []uint64{
			0,
			tree[0].id, // dir1
			tree[2].id, // dir1/sub1
			tree[1].id, // dir2
			tree[3].id, // dir2/sub1
			tree[5].id, // dir2/sub1/subsub1
			tree[4].id, // dir2/sub2
		}
		require.Equal(t, expected, actual)
	})
}

var errSubTreeSend = errors.New("test error")

type subTreeAcc struct {
	grpc.ServerStream // to satisfy the interface
	// IDs of the seen nodes.
	seen     []*GetSubTreeResponse
	errIndex int
}

func (s *subTreeAcc) Send(r *GetSubTreeResponse) error {
	s.seen = append(s.seen, r)
	if s.errIndex >= 0 {
		if len(s.seen) == s.errIndex+1 {
			return errSubTreeSend
		}
		if s.errIndex >= 0 && len(s.seen) > s.errIndex {
			panic("called Send after an error was returned")
		}
	}
	return nil
}
