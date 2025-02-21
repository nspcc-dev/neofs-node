package object_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strconv"
	"testing"

	"github.com/google/uuid"
	. "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/tzhash/tz"
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

func assertMergeResult(t testing.TB, res, expRes []client.SearchResultItem, more, expMore bool, err error) {
	require.NoError(t, err)
	require.Len(t, res, len(expRes))
	require.EqualValues(t, len(expRes), cap(res))
	require.Equal(t, expRes, res)
	require.Equal(t, expMore, more)
}

func testSysAttrOrder[T any](t *testing.T, attr string, vs []T, ss []string, toStr func(T) string, cmp func(a, b T) int) {
	require.True(t, slices.EqualFunc(vs, ss, func(v T, s string) bool { return toStr(v) == s }))
	require.True(t, slices.IsSortedFunc(vs, cmp))
	t.Run(attr, func(t *testing.T) {
		res, _, err := MergeSearchResults(1000, attr, false, [][]client.SearchResultItem{
			{{ID: oidtest.ID(), Attributes: []string{ss[1]}}},
			{{ID: oidtest.ID(), Attributes: []string{ss[3]}}},
			{{ID: oidtest.ID(), Attributes: []string{ss[2]}}},
			{{ID: oidtest.ID(), Attributes: []string{ss[0]}}},
		}, nil)
		require.NoError(t, err)
		require.Len(t, res, len(vs))
		for i := range res {
			require.Equal(t, []string{toStr(vs[i])}, res[i].Attributes)
		}
	})
}

func TestMergeSearchResults(t *testing.T) {
	t.Run("failures", func(t *testing.T) {
		t.Run("non-int attribute", func(t *testing.T) {
			_, _, err := MergeSearchResults(1000, "any", true, [][]client.SearchResultItem{
				{{ID: oidtest.ID(), Attributes: []string{"1"}}, {ID: oidtest.ID(), Attributes: []string{"2"}}},
				{{ID: oidtest.ID(), Attributes: []string{"2"}}, {ID: oidtest.ID(), Attributes: []string{"3"}}},
				{{ID: oidtest.ID(), Attributes: []string{"3"}}, {ID: oidtest.ID(), Attributes: []string{"four"}}},
			}, nil)
			require.EqualError(t, err, "non-int attribute in result #2")
		})
	})
	t.Run("zero limit", func(t *testing.T) {
		res, more, err := MergeSearchResults(0, "", false, [][]client.SearchResultItem{searchResultFromIDs(2)}, nil)
		require.NoError(t, err)
		require.Nil(t, res)
		require.False(t, more)
	})
	t.Run("no sets", func(t *testing.T) {
		res, more, err := MergeSearchResults(1000, "", false, nil, nil)
		require.NoError(t, err)
		require.Nil(t, res)
		require.False(t, more)
	})
	t.Run("empty sets only", func(t *testing.T) {
		res, more, err := MergeSearchResults(1000, "", false, make([][]client.SearchResultItem, 1000), nil)
		require.NoError(t, err)
		require.Empty(t, res)
		require.False(t, more)
		t.Run("single", func(t *testing.T) {
			// https://github.com/nspcc-dev/neofs-node/issues/3154
			expRes := make([]client.SearchResultItem, 0)
			res, more, err := MergeSearchResults(1, "", false, [][]client.SearchResultItem{expRes}, nil)
			assertMergeResult(t, res, expRes, more, false, err)
		})
	})
	t.Run("single set", func(t *testing.T) {
		expRes := searchResultFromIDs(4)
		t.Run("less than limit", func(t *testing.T) {
			res, more, err := MergeSearchResults(5, "", false, [][]client.SearchResultItem{expRes}, []bool{false})
			assertMergeResult(t, res, expRes, more, false, err)
		})
		t.Run("exactly limit", func(t *testing.T) {
			t.Run("no more", func(t *testing.T) {
				res, more, err := MergeSearchResults(4, "", false, [][]client.SearchResultItem{expRes}, []bool{false})
				assertMergeResult(t, res, expRes, more, false, err)
			})
			t.Run("more", func(t *testing.T) {
				res, more, err := MergeSearchResults(4, "", false, [][]client.SearchResultItem{expRes}, []bool{true})
				assertMergeResult(t, res, expRes, more, true, err)
			})
		})
		t.Run("more than limit", func(t *testing.T) {
			t.Run("no more", func(t *testing.T) {
				res, more, err := MergeSearchResults(3, "", false, [][]client.SearchResultItem{expRes}, []bool{false})
				require.Len(t, res, 3)
				assertMergeResult(t, res[:3:3], expRes[:3], more, true, err)
			})
			t.Run("more", func(t *testing.T) {
				res, more, err := MergeSearchResults(3, "", false, [][]client.SearchResultItem{expRes}, []bool{true})
				require.Len(t, res, 3)
				assertMergeResult(t, res[:3:3], expRes[:3], more, true, err)
			})
		})
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
		res, more, err := MergeSearchResults(1000, "any", true, sets, nil)
		assertMergeResult(t, res, all, more, false, err)
	})
	t.Run("concat", func(t *testing.T) {
		t.Run("no attributes", func(t *testing.T) {
			all := searchResultFromIDs(10)
			var sets [][]client.SearchResultItem
			for i := range len(all) / 2 {
				sets = append(sets, []client.SearchResultItem{all[2*i], all[2*i+1]})
			}
			res, more, err := MergeSearchResults(1000, "", false, sets, nil)
			assertMergeResult(t, res, all, more, false, err)
			t.Run("reverse", func(t *testing.T) {
				var sets [][]client.SearchResultItem
				for i := range len(all) / 2 {
					sets = append(sets, []client.SearchResultItem{all[2*i], all[2*i+1]})
				}
				slices.Reverse(sets)
				res, more, err := MergeSearchResults(1000, "", false, sets, nil)
				assertMergeResult(t, res, all, more, false, err)
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
			res, more, err := MergeSearchResults(1000, "any", true, sets, nil)
			assertMergeResult(t, res, all, more, false, err)
			t.Run("reverse", func(t *testing.T) {
				var sets [][]client.SearchResultItem
				for i := range len(all) / 2 {
					sets = append(sets, []client.SearchResultItem{all[2*i], all[2*i+1]})
				}
				slices.Reverse(sets)
				res, more, err := MergeSearchResults(1000, "any", true, sets, nil)
				assertMergeResult(t, res, all, more, false, err)
			})
		})
	})
	t.Run("intersecting", func(t *testing.T) {
		all := searchResultFromIDs(10)
		var sets [][]client.SearchResultItem
		for i := range len(all) - 1 {
			sets = append(sets, []client.SearchResultItem{all[i], all[i+1]})
		}
		res, more, err := MergeSearchResults(1000, "", false, sets, nil)
		assertMergeResult(t, res, all, more, false, err)
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
			res, more, err := MergeSearchResults(1000, "any", true, sets, nil)
			assertMergeResult(t, res, all, more, false, err)
		})
	})
	t.Run("cursors", func(t *testing.T) {
		all := searchResultFromIDs(10)
		t.Run("more items in last set", func(t *testing.T) {
			res, more, err := MergeSearchResults(5, "", false, [][]client.SearchResultItem{
				all[:3],
				all[:6],
				all[:2],
			}, nil)
			assertMergeResult(t, res, all[:5], more, true, err)
		})
		t.Run("more items in other set", func(t *testing.T) {
			res, more, err := MergeSearchResults(5, "", false, [][]client.SearchResultItem{
				all[:3],
				all[:5],
				all,
			}, nil)
			assertMergeResult(t, res, all[:5], more, true, err)
		})
		t.Run("flag", func(t *testing.T) {
			res, more, err := MergeSearchResults(5, "", false, [][]client.SearchResultItem{
				all[:1],
				all[:5],
				all[:2],
			}, []bool{
				true,
				false,
				false,
			})
			assertMergeResult(t, res, all[:5], more, true, err)
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
			res, more, err := MergeSearchResults(uint16(len(all)), "any", true, sets, nil)
			assertMergeResult(t, res, all, more, false, err)
		}
		t.Run("mixed", func(t *testing.T) {
			vals := []string{"-11", "-1a", "-1", "a0", "0", "0a", "1", "11", "1!", "2", "2!", "22"}
			slices.Sort(vals)
			all := searchResultFromIDs(len(vals))
			slices.Reverse(all)
			for i := range all {
				all[i].Attributes = []string{vals[i]}
			}
			for _, sets := range [][][]client.SearchResultItem{
				{all},
				{all[:len(all)/2], all[len(all)/2:]},
				{all[len(all)/2:], all[:len(all)/2]},
				{all[6:7], all[0:1], all[5:], all[1:2], all[4:5], all[2:3], all[3:4]},
				{all[5:], all[1:3], all[0:4], all[3:]},
			} {
				res, more, err := MergeSearchResults(uint16(len(all)), "any", false, sets, nil)
				assertMergeResult(t, res, all, more, false, err)
			}
		})
	})
	t.Run("system attributes", func(t *testing.T) {
		t.Run("OID", func(t *testing.T) { // https://github.com/nspcc-dev/neofs-node/issues/3160
			ids := []oid.ID{
				{6, 66, 212, 15, 99, 92, 193, 89, 165, 111, 36, 160, 35, 150, 126, 177, 208, 51, 229, 148, 1, 245, 188, 147, 68, 92, 227, 128, 184, 49, 150, 25},
				{83, 155, 1, 16, 139, 16, 27, 84, 238, 110, 215, 181, 245, 231, 129, 220, 192, 80, 168, 236, 35, 215, 29, 238, 133, 31, 176, 13, 250, 67, 126, 185},
				{84, 187, 66, 103, 55, 176, 48, 220, 171, 101, 83, 187, 75, 89, 244, 128, 14, 43, 160, 118, 226, 60, 180, 113, 95, 41, 15, 27, 151, 143, 183, 187},
				{154, 156, 84, 7, 36, 243, 19, 205, 118, 179, 244, 56, 251, 80, 184, 244, 97, 142, 113, 120, 167, 50, 111, 94, 219, 78, 151, 180, 89, 102, 52, 15},
			}
			ss := []string{
				"RSYscGLzKw1nkeVRGpowYTGgtgodXJrMyyiHTGGJW3S",
				"6dMvfyLF7HZ1WsBRgrLUDZP4pLkvNRjB6HWGeNXP4fJp",
				"6hkrsFBPpAKTAKHeC5gycCZsz2BQdKtAn9ADriNdWf4E",
				"BQY3VShN1BmU6XDKiQaDo2tk7s7rkYuaGeVgmcHcWsRY",
			}
			testSysAttrOrder(t, object.FilterFirstSplitObject, ids, ss, oid.ID.String, func(a, b oid.ID) int { return bytes.Compare(a[:], b[:]) })
			testSysAttrOrder(t, object.FilterParentID, ids, ss, oid.ID.String, func(a, b oid.ID) int { return bytes.Compare(a[:], b[:]) })
		})
		t.Run("owner", func(t *testing.T) {
			ids := []user.ID{
				{53, 34, 51, 0, 175, 224, 11, 194, 105, 251, 43, 64, 207, 48, 137, 106, 139, 238, 83, 152, 62, 77, 147, 169, 144},
				{53, 141, 8, 139, 207, 47, 176, 93, 67, 21, 172, 70, 17, 75, 74, 14, 251, 45, 183, 181, 172, 143, 189, 124, 127},
				{53, 204, 4, 7, 232, 10, 108, 49, 82, 9, 202, 19, 182, 191, 45, 208, 94, 251, 218, 49, 248, 24, 101, 80, 61},
				{53, 228, 192, 63, 41, 253, 147, 85, 97, 157, 242, 228, 124, 58, 7, 154, 129, 147, 90, 181, 221, 7, 104, 153, 251},
			}
			ss := []string{
				"NP2oG1M2X4aKu9NbC3svam4kfw8sG9Lue7",
				"NYmghbjDYzysGnqm3X2aKDZd8ZBkRpYMAW",
				"NeWhrxe5ZDbVF4Jo198PyLxHiFt6LmwFGG",
				"NgmVZ2j6MTt694WbFG86xnt1QU18xSsUpA",
			}
			testSysAttrOrder(t, object.FilterOwnerID, ids, ss, user.ID.String, func(a, b user.ID) int { return bytes.Compare(a[:], b[:]) })
		})
		t.Run("payload checksum", func(t *testing.T) {
			hs := [][sha256.Size]byte{
				{54, 219, 29, 225, 236, 168, 192, 139, 203, 57, 217, 32, 30, 80, 240, 1, 253, 255, 203, 35, 21, 100, 247, 150, 254, 57, 92, 122, 129, 6, 139, 48},
				{203, 244, 182, 131, 112, 136, 180, 95, 47, 21, 187, 188, 170, 220, 227, 241, 213, 111, 159, 63, 177, 52, 215, 239, 233, 139, 30, 164, 34, 219, 130, 17},
				{237, 220, 116, 42, 90, 242, 232, 57, 161, 62, 127, 221, 4, 18, 59, 159, 117, 190, 45, 16, 232, 152, 179, 48, 56, 62, 177, 72, 56, 73, 40, 152},
				{239, 85, 82, 153, 122, 189, 112, 44, 114, 89, 187, 112, 37, 38, 100, 110, 147, 187, 19, 186, 104, 22, 205, 226, 72, 180, 35, 216, 63, 23, 103, 117},
			}
			strs := []string{
				"36db1de1eca8c08bcb39d9201e50f001fdffcb231564f796fe395c7a81068b30",
				"cbf4b6837088b45f2f15bbbcaadce3f1d56f9f3fb134d7efe98b1ea422db8211",
				"eddc742a5af2e839a13e7fdd04123b9f75be2d10e898b330383eb14838492898",
				"ef5552997abd702c7259bb702526646e93bb13ba6816cde248b423d83f176775",
			}
			testSysAttrOrder(t, object.FilterPayloadChecksum, hs, strs, func(v [sha256.Size]byte) string {
				return hex.EncodeToString(v[:])
			}, func(a, b [sha256.Size]byte) int { return bytes.Compare(a[:], b[:]) })
		})
		t.Run("payload homomorphic checksum", func(t *testing.T) {
			hs := [][tz.Size]byte{
				{15, 61, 236, 2, 174, 213, 98, 116, 121, 254, 109, 210, 127, 88, 181, 151, 243, 45, 50, 14, 55, 83, 69, 11, 109, 13,
					6, 78, 232, 127, 107, 185, 170, 100, 162, 239, 34, 121, 65, 0, 127, 246, 239, 127, 62, 29, 131, 65, 72, 255, 29, 39, 13, 24, 213, 110, 221, 113, 230, 207, 111, 199, 198, 222},
				{105, 8, 109, 196, 241, 126, 245, 93, 101, 223, 151, 36, 50, 209, 135, 120, 57, 156, 236, 239, 215, 56, 35, 139,
					70, 94, 87, 89, 161, 6, 52, 75, 141, 5, 189, 230, 1, 72, 13, 179, 217, 233, 93, 11, 19, 199, 45, 147, 148, 69, 112, 111, 68, 129, 127, 127, 153, 129, 59, 183, 41, 13, 182, 80},
				{136, 82, 177, 151, 44, 101, 103, 188, 16, 175, 29, 150, 73, 131, 26, 243, 61, 190, 64, 65, 198, 70, 56, 245, 205,
					2, 79, 252, 215, 92, 128, 79, 103, 131, 15, 199, 82, 221, 171, 29, 208, 122, 56, 10, 159, 209, 157, 171, 15, 239, 17, 11, 126, 167, 78, 177, 226, 89, 213, 88, 18, 246, 212, 134},
				{254, 122, 0, 45, 98, 143, 230, 148, 44, 101, 111, 56, 116, 71, 173, 90, 100, 162, 180, 188, 189, 3, 131, 86, 114,
					155, 58, 190, 0, 0, 94, 146, 82, 9, 221, 224, 129, 250, 60, 239, 236, 173, 71, 78, 231, 109, 103, 36, 4, 188, 165, 21, 244, 22, 206, 45, 129, 66, 64, 107, 160, 86, 210, 86},
			}
			strs := []string{
				"0f3dec02aed5627479fe6dd27f58b597f32d320e3753450b6d0d064ee87f6bb9aa64a2ef227941007ff6ef7f3e1d834148ff1d270d18d56edd71e6cf6fc7c6de",
				"69086dc4f17ef55d65df972432d18778399cecefd738238b465e5759a106344b8d05bde601480db3d9e95d0b13c72d939445706f44817f7f99813bb7290db650",
				"8852b1972c6567bc10af1d9649831af33dbe4041c64638f5cd024ffcd75c804f67830fc752ddab1dd07a380a9fd19dab0fef110b7ea74eb1e259d55812f6d486",
				"fe7a002d628fe6942c656f387447ad5a64a2b4bcbd038356729b3abe00005e925209dde081fa3cefecad474ee76d672404bca515f416ce2d8142406ba056d256",
			}
			testSysAttrOrder(t, object.FilterPayloadChecksum, hs, strs, func(v [tz.Size]byte) string {
				return hex.EncodeToString(v[:])
			}, func(a, b [tz.Size]byte) int { return bytes.Compare(a[:], b[:]) })
		})
		t.Run("split ID", func(t *testing.T) {
			hs := []uuid.UUID{
				{52, 145, 222, 165, 88, 163, 77, 225, 151, 245, 3, 165, 215, 161, 254, 28},
				{185, 174, 187, 26, 252, 235, 69, 196, 174, 31, 235, 139, 148, 95, 197, 121},
				{218, 115, 5, 210, 123, 30, 78, 177, 176, 105, 217, 249, 142, 196, 80, 249},
				{221, 203, 97, 19, 249, 17, 76, 168, 182, 231, 212, 140, 127, 16, 24, 204},
			}
			strs := []string{
				"3491dea5-58a3-4de1-97f5-03a5d7a1fe1c",
				"b9aebb1a-fceb-45c4-ae1f-eb8b945fc579",
				"da7305d2-7b1e-4eb1-b069-d9f98ec450f9",
				"ddcb6113-f911-4ca8-b6e7-d48c7f1018cc",
			}
			testSysAttrOrder(t, object.FilterSplitID, hs, strs, uuid.UUID.String, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
		})
	})
}
