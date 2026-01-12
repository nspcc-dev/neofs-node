package engine

import (
	"slices"
	"testing"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func TestMergeOIDs(t *testing.T) {
	var cnr = cid.ID{0xff}

	var equalAddresses = func(a, b oid.Address) bool {
		return a.Compare(b) == 0
	}

	t.Run("nil list", func(t *testing.T) {
		require.Nil(t, mergeOIDs(cnr, nil))
	})
	t.Run("empty list", func(t *testing.T) {
		require.Nil(t, mergeOIDs(cnr, make([][]oid.ID, 0)))
	})
	t.Run("empty elements", func(t *testing.T) {
		require.Nil(t, mergeOIDs(cnr, make([][]oid.ID, 4)))
	})
	t.Run("single list", func(t *testing.T) {
		var (
			expected []oid.Address
			ids      = []oid.ID{{1}, {2}, {3}}
		)
		for _, id := range ids {
			expected = append(expected, oid.NewAddress(cnr, id))
		}
		require.Equal(t, expected, mergeOIDs(cnr, [][]oid.ID{ids}))
	})
	t.Run("two lists", func(t *testing.T) {
		var (
			expected []oid.Address
			ids1     = []oid.ID{{1}, {2}, {3}}
			ids2     = []oid.ID{{4}, {5}, {6}}
		)
		for _, id := range ids1 {
			expected = append(expected, oid.NewAddress(cnr, id))
		}
		for _, id := range ids2 {
			expected = append(expected, oid.NewAddress(cnr, id))
		}
		require.Equal(t, expected, mergeOIDs(cnr, [][]oid.ID{ids1, ids2}))
	})
	t.Run("two mixed lists", func(t *testing.T) {
		var (
			expected []oid.Address
			ids1     = []oid.ID{{1}, {3}, {5}}
			ids2     = []oid.ID{{2}, {4}, {6}, {7}}
		)
		for _, id := range ids1 {
			expected = append(expected, oid.NewAddress(cnr, id))
		}
		for _, id := range ids2 {
			expected = append(expected, oid.NewAddress(cnr, id))
		}
		slices.SortFunc(expected, oid.Address.Compare)
		require.Equal(t, expected, mergeOIDs(cnr, [][]oid.ID{ids1, ids2}))
	})
	t.Run("two lists with dups", func(t *testing.T) {
		var (
			expected []oid.Address
			ids1     = []oid.ID{{1}, {2}, {3}}
			ids2     = []oid.ID{{2}, {3}, {4}, {5}}
		)
		for _, id := range ids1 {
			expected = append(expected, oid.NewAddress(cnr, id))
		}
		for _, id := range ids2 {
			expected = append(expected, oid.NewAddress(cnr, id))
		}
		slices.SortFunc(expected, oid.Address.Compare)
		expected = slices.CompactFunc(expected, equalAddresses)
		require.Len(t, expected, 5) // Ensure sort/compact.
		require.Equal(t, expected, mergeOIDs(cnr, [][]oid.ID{ids1, ids2}))
	})
	t.Run("four lists with dups", func(t *testing.T) {
		var (
			expected []oid.Address
			ids1     = []oid.ID{{1}, {2}, {3}}
			ids2     = []oid.ID{{2}, {3}, {4}, {5}}
			ids3     = []oid.ID{{3}, {4}, {5}}
			ids4     = []oid.ID{{1}, {4}, {6}, {7}}
			idz      = [][]oid.ID{ids1, ids2, ids3, ids4}
		)
		for _, ids := range idz {
			for _, id := range ids {
				expected = append(expected, oid.NewAddress(cnr, id))
			}
		}
		slices.SortFunc(expected, oid.Address.Compare)
		expected = slices.CompactFunc(expected, equalAddresses)
		require.Len(t, expected, 7) // Ensure sort/compact.
		require.Equal(t, expected, mergeOIDs(cnr, idz))
	})
}
