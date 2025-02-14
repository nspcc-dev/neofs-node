package object_test

import (
	"bytes"
	"slices"
	"strconv"
	"testing"

	. "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func searchResultFromIDs(n int) []client.SearchResultItem {
	ids := oidtest.IDs(n)
	s := make([]client.SearchResultItem, len(ids))
	for i := range ids {
		s[i].ID = ids[i]
	}
	slices.SortFunc(s, func(a, b client.SearchResultItem) int { return bytes.Compare(a.ID[:], b.ID[:]) })
	return s
}

func assertMergeResult(t testing.TB, res, expRes []client.SearchResultItem, more, expMore bool) {
	require.Len(t, res, len(expRes))
	require.EqualValues(t, len(expRes), cap(res))
	require.Equal(t, expRes, res)
	require.Equal(t, expMore, more)
}

func TestMergeSearchResults(t *testing.T) {
	t.Run("zero limit", func(t *testing.T) {
		res, more := MergeSearchResults(0, false, [][]client.SearchResultItem{searchResultFromIDs(2)}, nil)
		require.Nil(t, res)
		require.False(t, more)
	})
	t.Run("no sets", func(t *testing.T) {
		res, more := MergeSearchResults(1000, false, nil, nil)
		require.Nil(t, res)
		require.False(t, more)
	})
	t.Run("empty sets only", func(t *testing.T) {
		res, more := MergeSearchResults(1000, false, make([][]client.SearchResultItem, 1000), nil)
		require.Empty(t, res)
		require.False(t, more)
	})
	t.Run("with empty sets", func(t *testing.T) {
		all := []client.SearchResultItem{
			{ID: oidtest.ID(), Attributes: []string{"12", "d"}},
			{ID: oidtest.ID(), Attributes: []string{"23", "c"}},
			{ID: oidtest.ID(), Attributes: []string{"34", "b"}},
			{ID: oidtest.ID(), Attributes: []string{"45", "a"}},
		}
		sets := [][]client.SearchResultItem{
			nil,
			{all[0], all[2]},
			{},
			{all[1], all[3]},
			nil,
			all,
		}
		res, more := MergeSearchResults(1000, true, sets, nil)
		assertMergeResult(t, res, all, more, false)
	})
	t.Run("concat", func(t *testing.T) {
		t.Run("no attributes", func(t *testing.T) {
			all := searchResultFromIDs(10)
			var sets [][]client.SearchResultItem
			for i := range len(all) / 2 {
				sets = append(sets, []client.SearchResultItem{all[2*i], all[2*i+1]})
			}
			res, more := MergeSearchResults(1000, false, sets, nil)
			assertMergeResult(t, res, all, more, false)
			t.Run("reverse", func(t *testing.T) {
				var sets [][]client.SearchResultItem
				for i := range len(all) / 2 {
					sets = append(sets, []client.SearchResultItem{all[2*i], all[2*i+1]})
				}
				slices.Reverse(sets)
				res, more := MergeSearchResults(1000, false, sets, nil)
				assertMergeResult(t, res, all, more, false)
			})
		})
		t.Run("with attributes", func(t *testing.T) {
			all := searchResultFromIDs(10)
			slices.Reverse(all)
			for i := range all {
				all[i].Attributes = []string{strconv.Itoa(i)}
			}
			var sets [][]client.SearchResultItem
			for i := range len(all) / 2 {
				sets = append(sets, []client.SearchResultItem{all[2*i], all[2*i+1]})
			}
			res, more := MergeSearchResults(1000, true, sets, nil)
			assertMergeResult(t, res, all, more, false)
			t.Run("reverse", func(t *testing.T) {
				var sets [][]client.SearchResultItem
				for i := range len(all) / 2 {
					sets = append(sets, []client.SearchResultItem{all[2*i], all[2*i+1]})
				}
				slices.Reverse(sets)
				res, more := MergeSearchResults(1000, true, sets, nil)
				assertMergeResult(t, res, all, more, false)
			})
		})
	})
	t.Run("intersecting", func(t *testing.T) {
		all := searchResultFromIDs(10)
		var sets [][]client.SearchResultItem
		for i := range len(all) - 1 {
			sets = append(sets, []client.SearchResultItem{all[i], all[i+1]})
		}
		res, more := MergeSearchResults(1000, false, sets, nil)
		assertMergeResult(t, res, all, more, false)
		t.Run("with attributes", func(t *testing.T) {
			all := searchResultFromIDs(10)
			slices.Reverse(all)
			for i := range all {
				all[i].Attributes = []string{strconv.Itoa(i)}
			}
			var sets [][]client.SearchResultItem
			for i := range len(all) - 1 {
				sets = append(sets, []client.SearchResultItem{all[i], all[i+1]})
			}
			res, more := MergeSearchResults(1000, true, sets, nil)
			assertMergeResult(t, res, all, more, false)
		})
	})
	t.Run("cursors", func(t *testing.T) {
		all := searchResultFromIDs(10)
		t.Run("more items in last set", func(t *testing.T) {
			res, more := MergeSearchResults(5, false, [][]client.SearchResultItem{
				all[:3],
				all[:6],
				all[:2],
			}, nil)
			assertMergeResult(t, res, all[:5], more, true)
		})
		t.Run("more items in other set", func(t *testing.T) {
			res, more := MergeSearchResults(5, false, [][]client.SearchResultItem{
				all[:3],
				all[:5],
				all,
			}, nil)
			assertMergeResult(t, res, all[:5], more, true)
		})
		t.Run("flag", func(t *testing.T) {
			res, more := MergeSearchResults(5, false, [][]client.SearchResultItem{
				all[:1],
				all[:5],
				all[:2],
			}, []bool{
				true,
				false,
				false,
			})
			assertMergeResult(t, res, all[:5], more, true)
		})
	})
	t.Run("integers", func(t *testing.T) {
		vals := []string{
			"-111111111111111111111111111111111111111111111111111111",
			"-18446744073709551615",
			"-1", "0", "1",
			"18446744073709551615",
			"111111111111111111111111111111111111111111111111111111",
		}
		all := searchResultFromIDs(len(vals))
		slices.Reverse(all)
		for i := range all {
			all[i].Attributes = []string{vals[i]}
		}
		for _, sets := range [][][]client.SearchResultItem{
			{all},
			{all[:len(all)/2], all[len(all)/2:]},
			{all[len(all)/2:], all[:len(all)/2]},
			{all[6:7], all[0:1], all[5:6], all[1:2], all[4:5], all[2:3], all[3:4]},
			{all[5:], all[1:3], all[0:4], all[3:]},
		} {
			res, more := MergeSearchResults(uint16(len(all)), true, sets, nil)
			assertMergeResult(t, res, all, more, false)
		}
	})
}
