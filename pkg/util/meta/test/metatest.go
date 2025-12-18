package metatest

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"testing"

	"github.com/google/uuid"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
)

type DB interface {
	Put(obj *object.Object) error
	Search(cnr cid.ID, fs []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error)
}

func TestSearchObjects(t *testing.T, db DB, testSplitID bool) {
	t.Run("filters", func(t *testing.T) {
		// this test is focused on correct filters' application only, so only sorting by
		// IDs is checked
		const nRoot = 2
		const nPhy = 2 * nRoot
		const nAll = nRoot + nPhy
		all := []uint{0, 1, 2, 3, 4, 5}
		group1 := []uint{0, 2, 4}
		group2 := []uint{1, 3, 5}
		ids := [nAll]oid.ID{
			// RSYscGLzKw1nkeVRGpowYTGgtgodXJrMyyiHTGGJW3S
			{6, 66, 212, 15, 99, 92, 193, 89, 165, 111, 36, 160, 35, 150, 126, 177, 208, 51, 229, 148, 1, 245, 188, 147, 68, 92, 227, 128, 184, 49, 150, 25},
			// 6dMvfyLF7HZ1WsBRgrLUDZP4pLkvNRjB6HWGeNXP4fJp
			{83, 155, 1, 16, 139, 16, 27, 84, 238, 110, 215, 181, 245, 231, 129, 220, 192, 80, 168, 236, 35, 215, 29, 238, 133, 31, 176, 13, 250, 67, 126, 185},
			// 6hkrsFBPpAKTAKHeC5gycCZsz2BQdKtAn9ADriNdWf4E
			{84, 187, 66, 103, 55, 176, 48, 220, 171, 101, 83, 187, 75, 89, 244, 128, 14, 43, 160, 118, 226, 60, 180, 113, 95, 41, 15, 27, 151, 143, 183, 187},
			// BQY3VShN1BmU6XDKiQaDo2tk7s7rkYuaGeVgmcHcWsRY
			{154, 156, 84, 7, 36, 243, 19, 205, 118, 179, 244, 56, 251, 80, 184, 244, 97, 142, 113, 120, 167, 50, 111, 94, 219, 78, 151, 180, 89, 102, 52, 15},
			// DsKLie7U2BVph5XkZttG8EERxt9DFQXkrowr6LFkxp8h
			{191, 48, 5, 72, 64, 44, 163, 71, 127, 144, 18, 30, 134, 67, 189, 210, 243, 2, 101, 225, 63, 47, 174, 128, 41, 238, 107, 14, 87, 136, 50, 162},
			// Gv9XcEW7KREB8cnjFbW8HBdJesMnbNKknfGdBNsVtQmB
			{236, 124, 186, 165, 234, 207, 5, 237, 62, 82, 41, 15, 133, 132, 132, 73, 55, 16, 69, 101, 214, 174, 160, 228, 101, 161, 18, 204, 241, 208, 155, 118},
		}
		// HYFTEXkzpDWkXU6anQByuSPvV3imjzTKJBaAyD4VYg23
		cnr := cid.ID{245, 188, 86, 80, 170, 97, 147, 48, 75, 27, 115, 238, 61, 151, 182, 191, 95, 33, 160, 253, 239, 70, 174, 188, 220, 84, 57, 222, 9, 104, 4, 48}
		// cnrStr := "HYFTEXkzpDWkXU6anQByuSPvV3imjzTKJBaAyD4VYg23"
		owners := [nRoot]user.ID{
			// NfzJyPrn1hRGuVJNvMYLTfWZGW2ZVR9Qmj
			{53, 220, 52, 178, 96, 0, 121, 121, 217, 160, 223, 119, 75, 71, 2, 233, 33, 138, 241, 182, 208, 164, 240, 222, 30},
			// NiUWeE8gb8njJmymdZTh229ojGeJ24WHSm
			{53, 247, 122, 86, 36, 254, 120, 76, 10, 73, 62, 4, 132, 174, 224, 77, 32, 37, 224, 73, 102, 37, 121, 117, 46},
		}
		checksums := [nAll][32]byte{
			// 8a61b9ff3de0983ed7ad7aa21db22ff91e5a2a07128cd45e3646282f90e4efd7
			{138, 97, 185, 255, 61, 224, 152, 62, 215, 173, 122, 162, 29, 178, 47, 249, 30, 90, 42, 7, 18, 140, 212, 94, 54, 70, 40, 47, 144, 228, 239, 215},
			// d501baff2dec96b7dec7d634e5ec13ed8be33048bfa4e8285a37dabc0537e677
			{213, 1, 186, 255, 45, 236, 150, 183, 222, 199, 214, 52, 229, 236, 19, 237, 139, 227, 48, 72, 191, 164, 232, 40, 90, 55, 218, 188, 5, 55, 230, 119},
			// 302b0610844a4da6874f566798018e9d79031a4cc8bf72357d8fc5413a54473e
			{48, 43, 6, 16, 132, 74, 77, 166, 135, 79, 86, 103, 152, 1, 142, 157, 121, 3, 26, 76, 200, 191, 114, 53, 125, 143, 197, 65, 58, 84, 71, 62},
			// 9bcee80d024eb36a3dbb8e7948d1a9b672a82929950a85ccd350e31e34560672
			{155, 206, 232, 13, 2, 78, 179, 106, 61, 187, 142, 121, 72, 209, 169, 182, 114, 168, 41, 41, 149, 10, 133, 204, 211, 80, 227, 30, 52, 86, 6, 114},
			// 35d6c9f1aa664aa163f2ec0bffe48af0bd4e8bc640626c12759f187876007529
			{53, 214, 201, 241, 170, 102, 74, 161, 99, 242, 236, 11, 255, 228, 138, 240, 189, 78, 139, 198, 64, 98, 108, 18, 117, 159, 24, 120, 118, 0, 117, 41},
			// cc6c36b379e9a77a845a021498e2e92875131af404f825aa56bea91602785ef2
			{204, 108, 54, 179, 121, 233, 167, 122, 132, 90, 2, 20, 152, 226, 233, 40, 117, 19, 26, 244, 4, 248, 37, 170, 86, 190, 169, 22, 2, 120, 94, 242},
		}
		hmmChecksums := [nAll][64]byte{
			// a73a37d54475df580b324d70f3d1ac922200af91f196dd9cb0f8f1cca5fefdf0cb3dbc4aaac639416e3fdd4c540e616e6b44ac6b56a3b194e8011925192a8be2
			{167, 58, 55, 213, 68, 117, 223, 88, 11, 50, 77, 112, 243, 209, 172, 146, 34, 0, 175, 145, 241, 150, 221, 156, 176, 248, 241, 204, 165, 254, 253, 240, 203, 61, 188, 74, 170, 198, 57, 65, 110, 63, 221, 76, 84, 14, 97, 110, 107, 68, 172, 107, 86, 163, 177, 148, 232, 1, 25, 37, 25, 42, 139, 226},
			// f72b6eb562c6dd5e69930ab51ca8a98b13bfa18013cd89df3254dbc615f86b8f8c042649fe76e01f54bea7216957fe6716ec0a33d6b6de25ec15a53f295196d1
			{247, 43, 110, 181, 98, 198, 221, 94, 105, 147, 10, 181, 28, 168, 169, 139, 19, 191, 161, 128, 19, 205, 137, 223, 50, 84, 219, 198, 21, 248, 107, 143, 140, 4, 38, 73, 254, 118, 224, 31, 84, 190, 167, 33, 105, 87, 254, 103, 22, 236, 10, 51, 214, 182, 222, 37, 236, 21, 165, 63, 41, 81, 150, 209},
			// 55a8577889ed275d15509b202b084fb7876c08408b8c61a1ba9ab26834f08c667ccde2acf55fcfc1755cb2a6f8316e1c6185bd48549b150767979cf76ede4b1c
			{85, 168, 87, 120, 137, 237, 39, 93, 21, 80, 155, 32, 43, 8, 79, 183, 135, 108, 8, 64, 139, 140, 97, 161, 186, 154, 178, 104, 52, 240, 140, 102, 124, 205, 226, 172, 245, 95, 207, 193, 117, 92, 178, 166, 248, 49, 110, 28, 97, 133, 189, 72, 84, 155, 21, 7, 103, 151, 156, 247, 110, 222, 75, 28},
			// 4d97f1f4f17119efae4579ef916ca1535e68c4fa381c431ab4112cb5671ddb21e44dc78f02ae2b26c95d5f74bb5eb4350e00cdc5b270f60bf46deaafc1b84575
			{77, 151, 241, 244, 241, 113, 25, 239, 174, 69, 121, 239, 145, 108, 161, 83, 94, 104, 196, 250, 56, 28, 67, 26, 180, 17, 44, 181, 103, 29, 219, 33, 228, 77, 199, 143, 2, 174, 43, 38, 201, 93, 95, 116, 187, 94, 180, 53, 14, 0, 205, 197, 178, 112, 246, 11, 244, 109, 234, 175, 193, 184, 69, 117},
			// 80089235980bfbf6c01a93c4f507b2f1ff2ec8b0c29cfe6970ce95cbeb1739bef6a43626783d58f56c224cfb606c360301f632a198db63f599fca7be2e0c2566
			{128, 8, 146, 53, 152, 11, 251, 246, 192, 26, 147, 196, 245, 7, 178, 241, 255, 46, 200, 176, 194, 156, 254, 105, 112, 206, 149, 203, 235, 23, 57, 190, 246, 164, 54, 38, 120, 61, 88, 245, 108, 34, 76, 251, 96, 108, 54, 3, 1, 246, 50, 161, 152, 219, 99, 245, 153, 252, 167, 190, 46, 12, 37, 102},
			// f3b6eedc3f30b99309582a7e0ca09dd6a9234ce95bfa578ddfa6ef2a0fe9c56e8f6a86c82ce565d9216c02110c0fe44079a68275243ad2f9be6bf7dacdeed97c
			{243, 182, 238, 220, 63, 48, 185, 147, 9, 88, 42, 126, 12, 160, 157, 214, 169, 35, 76, 233, 91, 250, 87, 141, 223, 166, 239, 42, 15, 233, 197, 110, 143, 106, 134, 200, 44, 229, 101, 217, 33, 108, 2, 17, 12, 15, 228, 64, 121, 166, 130, 117, 36, 58, 210, 249, 190, 107, 247, 218, 205, 238, 217, 124},
		}
		groupAttrs := [nRoot]object.Attribute{
			object.NewAttribute("group_attr_1", "group_val_1"),
			object.NewAttribute("group_attr_2", "group_val_2"),
		}
		types := [nRoot]object.Type{object.TypeRegular, object.TypeStorageGroup} //nolint:staticcheck // storage groups are deprecated, but this test needs some additional type.
		splitIDs := [nRoot][]byte{
			// 8b69e76d-5e95-4639-8213-46786c41ab73
			{139, 105, 231, 109, 94, 149, 70, 57, 130, 19, 70, 120, 108, 65, 171, 115},
			// 60c6b1ff-5e6d-4c0f-8699-15d54bf8a2e1
			{96, 198, 177, 255, 94, 109, 76, 15, 134, 153, 21, 213, 75, 248, 162, 225},
		}
		firstIDs := [nRoot]oid.ID{
			// 61hnJaKip8c1QxvC2iT4Txfpxf37QBNRaw1XCeq72DbC
			{74, 120, 139, 195, 149, 106, 19, 73, 151, 116, 227, 3, 83, 169, 108, 129, 20, 206, 146, 192, 140, 2, 85, 14, 244, 109, 247, 28, 51, 101, 212, 183},
			// Cdf8vnK5xTxmkdc1GcjkxaEQFtEmwHPRky4KRQik6rQH
			{172, 212, 150, 43, 17, 126, 75, 161, 99, 197, 238, 169, 62, 209, 96, 183, 79, 236, 237, 83, 141, 73, 125, 166, 186, 82, 68, 27, 147, 18, 24, 2},
		}

		initObj := func(obj *object.Object, nGlobal, nGroup int) {
			ver := version.New(100+uint32(nGroup), 200+uint32(nGroup))
			obj.SetVersion(&ver)
			obj.SetContainerID(cnr)
			obj.SetID(ids[nGlobal])
			obj.SetType(types[nGroup])
			obj.SetOwner(owners[nGroup])
			obj.SetCreationEpoch(10 + uint64(nGroup))
			obj.SetPayloadSize(20 + uint64(nGroup))
			obj.SetPayloadChecksum(checksum.NewSHA256(checksums[nGlobal]))
			obj.SetPayloadHomomorphicHash(checksum.NewTillichZemor(hmmChecksums[nGlobal]))
			si := strconv.Itoa(nGlobal)
			obj.SetAttributes(
				object.NewAttribute("attr_common", "val_common"),
				object.NewAttribute("unique_attr_"+si, "unique_val_"+si),
				groupAttrs[nGroup],
				object.NewAttribute("global_non_integer", "not an integer"),
			)
		}

		var pars [nRoot]object.Object
		for i := range nRoot {
			initObj(&pars[i], i, i)
		}

		var phys [nPhy]object.Object
		for i := range phys {
			nGroup := i % nRoot
			initObj(&phys[i], nRoot+i, nGroup)
			phys[i].SetSplitID(object.NewSplitIDFromV2(splitIDs[nGroup]))
			phys[i].SetFirstID(firstIDs[nGroup])
			phys[i].SetParent(&pars[nGroup])
		}

		appendAttribute(&pars[0], "attr_int", "-115792089237316195423570985008687907853269984665640564039457584007913129639935")
		appendAttribute(&phys[0], "attr_int", "-18446744073709551615")
		appendAttribute(&phys[1], "attr_int", "0")
		appendAttribute(&phys[2], "attr_int", "18446744073709551615")
		appendAttribute(&pars[1], "attr_int", "115792089237316195423570985008687907853269984665640564039457584007913129639935")

		for i := range phys {
			require.NoError(t, db.Put(&phys[i]))
		}

		check := func(t *testing.T, k string, m object.SearchMatchType, v string, matchInds []uint) {
			t.Run(fmt.Sprintf("%s %s", m, v), func(t *testing.T) {
				var fs object.SearchFilters
				fs.AddFilter(k, v, m)
				assertSearchResultIndexes(t, db, cnr, fs, nil, searchResultForIDs(ids[:]), matchInds)
			})
		}

		t.Run("all", func(t *testing.T) {
			check(t, "attr_common", object.MatchStringEqual, "val_common", all)
		})
		t.Run("user attributes", func(t *testing.T) {
			// unique
			for i := range all {
				si := strconv.Itoa(i)
				key := "unique_attr_" + si
				val := "unique_val_" + si
				check(t, key, object.MatchStringEqual, val, []uint{uint(i)})
				check(t, key, object.MatchStringNotEqual, "other_val", []uint{uint(i)})
				for j := range val {
					check(t, key, object.MatchCommonPrefix, val[:j], []uint{uint(i)})
				}
				for _, matcher := range []object.SearchMatchType{
					object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
				} {
					check(t, key, matcher, val, nil)
				}
				var others []uint
				for j := range all {
					if j != i {
						others = append(others, uint(j))
					}
				}
				check(t, key, object.MatchNotPresent, "", others)
			}
			// group
			const val1 = "group_val_1"
			check(t, "group_attr_1", object.MatchStringEqual, val1, group1)
			check(t, "group_attr_1", object.MatchStringNotEqual, val1, nil)
			check(t, "group_attr_1", object.MatchNotPresent, val1, group2)
			for i := range val1 {
				check(t, "group_attr_1", object.MatchCommonPrefix, val1[:i], group1)
			}
			const val2 = "group_val_2"
			check(t, "group_attr_2", object.MatchStringEqual, val2, group2)
			check(t, "group_attr_2", object.MatchStringNotEqual, val2, nil)
			check(t, "group_attr_2", object.MatchNotPresent, val2, group1)
			for i := range val1 {
				check(t, "group_attr_2", object.MatchCommonPrefix, val2[:i], group2)
			}
			for _, matcher := range []object.SearchMatchType{
				object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check(t, "group_attr_1", matcher, val1, nil)
				check(t, "group_attr_2", matcher, val2, nil)
			}
		})
		t.Run("ROOT", func(t *testing.T) {
			for _, matcher := range []object.SearchMatchType{
				object.MatchUnspecified, object.MatchStringEqual, object.MatchStringNotEqual,
				object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check(t, "$Object:ROOT", matcher, "", []uint{0})
			}
			check(t, "$Object:ROOT", object.MatchNotPresent, "", nil)
		})
		t.Run("PHY", func(t *testing.T) {
			for _, matcher := range []object.SearchMatchType{
				object.MatchUnspecified, object.MatchStringEqual, object.MatchStringNotEqual,
				object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check(t, "$Object:PHY", matcher, "", []uint{2, 3, 4, 5})
			}
			check(t, "$Object:PHY", object.MatchNotPresent, "", nil)
		})
		t.Run("version", func(t *testing.T) {
			check := func(m object.SearchMatchType, v string, matchInds []uint) {
				check(t, "$Object:version", m, v, matchInds)
			}
			check(object.MatchStringEqual, "v100.200", group1)
			check(object.MatchStringNotEqual, "v100.200", group2)
			check(object.MatchStringEqual, "v101.201", group2)
			check(object.MatchStringNotEqual, "v101.201", group1)
			check(object.MatchStringEqual, "v102.202", nil) // other
			check(object.MatchStringNotEqual, "v102.202", all)
			for _, m := range []object.SearchMatchType{
				object.MatchNotPresent, object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check(m, "v100.200", nil)
			}
			check(object.MatchCommonPrefix, "", all)
			check(object.MatchCommonPrefix, "v", all)
			check(object.MatchCommonPrefix, "v1", all)
			check(object.MatchCommonPrefix, "v10", all)
			check(object.MatchCommonPrefix, "v100", group1)
			check(object.MatchCommonPrefix, "v100.200", group1)
			check(object.MatchCommonPrefix, "v100.2001", nil)
			check(object.MatchCommonPrefix, "v101", group2)
			check(object.MatchCommonPrefix, "v101.201", group2)
			check(object.MatchCommonPrefix, "v2", nil)
		})
		t.Run("owner", func(t *testing.T) {
			check := func(m object.SearchMatchType, v string, matchInds []uint) {
				check(t, "$Object:ownerID", m, v, matchInds)
			}
			check(object.MatchStringEqual, "NfzJyPrn1hRGuVJNvMYLTfWZGW2ZVR9Qmj", group1)
			check(object.MatchStringNotEqual, "NfzJyPrn1hRGuVJNvMYLTfWZGW2ZVR9Qmj", group2)
			check(object.MatchStringEqual, "NiUWeE8gb8njJmymdZTh229ojGeJ24WHSm", group2)
			check(object.MatchStringNotEqual, "NiUWeE8gb8njJmymdZTh229ojGeJ24WHSm", group1)
			check(object.MatchStringEqual, "NhP5vErYP9WCfPjtCb78xqPV5MgHyhVNeL", nil) // other
			check(object.MatchStringNotEqual, "NhP5vErYP9WCfPjtCb78xqPV5MgHyhVNeL", all)
			for _, m := range []object.SearchMatchType{
				object.MatchNotPresent, object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check(m, "NfzJyPrn1hRGuVJNvMYLTfWZGW2ZVR9Qmj", nil)
			}
			check(object.MatchCommonPrefix, "N", all)
			check(object.MatchCommonPrefix, "Nf", group1)
			check(object.MatchCommonPrefix, "NfzJyPrn1hRGuVJNvMYLTfWZGW2ZVR9Qmj", group1)
			check(object.MatchCommonPrefix, "NfzJyPrn1hRGuVJNvMYLTfWZGW2ZVR9Qmj1", nil)
		})
		t.Run("type", func(t *testing.T) {
			check := func(m object.SearchMatchType, v string, matchInds []uint) {
				check(t, "$Object:objectType", m, v, matchInds)
			}
			check(object.MatchStringEqual, "REGULAR", group1)
			check(object.MatchStringNotEqual, "REGULAR", group2)
			check(object.MatchStringEqual, "STORAGE_GROUP", group2)
			check(object.MatchStringNotEqual, "STORAGE_GROUP", group1)
			check(object.MatchStringEqual, "STORAGE_GROUP", group2)
			check(object.MatchStringEqual, "TOMBSTONE", nil)
			check(object.MatchStringNotEqual, "TOMBSTONE", all)
			check(object.MatchStringEqual, "0", nil) // numeric enum value
			check(object.MatchStringEqual, "2", nil)
			for _, matcher := range []object.SearchMatchType{
				object.MatchNotPresent, object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check(matcher, "", nil)
				check(matcher, "TOMBSTONE", nil)
				check(matcher, "LOCK", nil)
				// check(matcher, "1", nil)
				// check(matcher, "3", nil)
			}
			check(object.MatchCommonPrefix, "", all)
			check(object.MatchCommonPrefix, "R", group1)
			check(object.MatchCommonPrefix, "S", group2)
			check(object.MatchCommonPrefix, "L", nil)
		})
		t.Run("payload checksum", func(t *testing.T) {
			check := func(m object.SearchMatchType, v string, matchInds []uint) {
				check(t, "$Object:payloadHash", m, v, matchInds)
			}
			check(object.MatchStringEqual, "8a61b9ff3de0983ed7ad7aa21db22ff91e5a2a07128cd45e3646282f90e4efd7", []uint{0})
			check(object.MatchStringEqual, "d501baff2dec96b7dec7d634e5ec13ed8be33048bfa4e8285a37dabc0537e677", []uint{1})
			check(object.MatchStringEqual, "302b0610844a4da6874f566798018e9d79031a4cc8bf72357d8fc5413a54473e", []uint{2})
			check(object.MatchStringEqual, "9bcee80d024eb36a3dbb8e7948d1a9b672a82929950a85ccd350e31e34560672", []uint{3})
			check(object.MatchStringEqual, "35d6c9f1aa664aa163f2ec0bffe48af0bd4e8bc640626c12759f187876007529", []uint{4})
			check(object.MatchStringEqual, "cc6c36b379e9a77a845a021498e2e92875131af404f825aa56bea91602785ef2", []uint{5})
			check(object.MatchStringNotEqual, "8a61b9ff3de0983ed7ad7aa21db22ff91e5a2a07128cd45e3646282f90e4efd7", []uint{1, 2, 3, 4, 5})
			check(object.MatchStringNotEqual, "d501baff2dec96b7dec7d634e5ec13ed8be33048bfa4e8285a37dabc0537e677", []uint{0, 2, 3, 4, 5})
			check(object.MatchStringNotEqual, "302b0610844a4da6874f566798018e9d79031a4cc8bf72357d8fc5413a54473e", []uint{0, 1, 3, 4, 5})
			check(object.MatchStringNotEqual, "9bcee80d024eb36a3dbb8e7948d1a9b672a82929950a85ccd350e31e34560672", []uint{0, 1, 2, 4, 5})
			check(object.MatchStringNotEqual, "35d6c9f1aa664aa163f2ec0bffe48af0bd4e8bc640626c12759f187876007529", []uint{0, 1, 2, 3, 5})
			check(object.MatchStringNotEqual, "cc6c36b379e9a77a845a021498e2e92875131af404f825aa56bea91602785ef2", []uint{0, 1, 2, 3, 4})
			check(object.MatchStringEqual, "cc6c36b379e9a77a845a021498e2e92875131af404f825aa56bea91602785ef1", nil) // other
			check(object.MatchStringNotEqual, "cc6c36b379e9a77a845a021498e2e92875131af404f825aa56bea91602785ef1", all)
			for _, m := range []object.SearchMatchType{
				object.MatchNotPresent, object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check(m, "8a61b9ff3de0983ed7ad7aa21db22ff91e5a2a07128cd45e3646282f90e4efd7", nil)
			}
			check(object.MatchCommonPrefix, "", all)
			check(object.MatchCommonPrefix, "8a", []uint{0})
			check(object.MatchCommonPrefix, "8a61b9ff3de0983ed7ad7aa21db22ff91e5a2a07128cd45e3646282f90e4efd7", []uint{0})
			check(object.MatchCommonPrefix, "4a", nil)
		})
		t.Run("payload homomorphic checksum", func(t *testing.T) {
			check := func(m object.SearchMatchType, v string, matchInds []uint) {
				check(t, "$Object:homomorphicHash", m, v, matchInds)
			}
			check(object.MatchStringEqual, "a73a37d54475df580b324d70f3d1ac922200af91f196dd9cb0f8f1cca5fefdf0cb3dbc4aaac639416e3fdd4c540e616e6b44ac6b56a3b194e8011925192a8be2", []uint{0})
			check(object.MatchStringEqual, "f72b6eb562c6dd5e69930ab51ca8a98b13bfa18013cd89df3254dbc615f86b8f8c042649fe76e01f54bea7216957fe6716ec0a33d6b6de25ec15a53f295196d1", []uint{1})
			check(object.MatchStringEqual, "55a8577889ed275d15509b202b084fb7876c08408b8c61a1ba9ab26834f08c667ccde2acf55fcfc1755cb2a6f8316e1c6185bd48549b150767979cf76ede4b1c", []uint{2})
			check(object.MatchStringEqual, "4d97f1f4f17119efae4579ef916ca1535e68c4fa381c431ab4112cb5671ddb21e44dc78f02ae2b26c95d5f74bb5eb4350e00cdc5b270f60bf46deaafc1b84575", []uint{3})
			check(object.MatchStringEqual, "80089235980bfbf6c01a93c4f507b2f1ff2ec8b0c29cfe6970ce95cbeb1739bef6a43626783d58f56c224cfb606c360301f632a198db63f599fca7be2e0c2566", []uint{4})
			check(object.MatchStringEqual, "f3b6eedc3f30b99309582a7e0ca09dd6a9234ce95bfa578ddfa6ef2a0fe9c56e8f6a86c82ce565d9216c02110c0fe44079a68275243ad2f9be6bf7dacdeed97c", []uint{5})
			check(object.MatchStringNotEqual, "a73a37d54475df580b324d70f3d1ac922200af91f196dd9cb0f8f1cca5fefdf0cb3dbc4aaac639416e3fdd4c540e616e6b44ac6b56a3b194e8011925192a8be2", []uint{1, 2, 3, 4, 5})
			check(object.MatchStringNotEqual, "f72b6eb562c6dd5e69930ab51ca8a98b13bfa18013cd89df3254dbc615f86b8f8c042649fe76e01f54bea7216957fe6716ec0a33d6b6de25ec15a53f295196d1", []uint{0, 2, 3, 4, 5})
			check(object.MatchStringNotEqual, "55a8577889ed275d15509b202b084fb7876c08408b8c61a1ba9ab26834f08c667ccde2acf55fcfc1755cb2a6f8316e1c6185bd48549b150767979cf76ede4b1c", []uint{0, 1, 3, 4, 5})
			check(object.MatchStringNotEqual, "4d97f1f4f17119efae4579ef916ca1535e68c4fa381c431ab4112cb5671ddb21e44dc78f02ae2b26c95d5f74bb5eb4350e00cdc5b270f60bf46deaafc1b84575", []uint{0, 1, 2, 4, 5})
			check(object.MatchStringNotEqual, "80089235980bfbf6c01a93c4f507b2f1ff2ec8b0c29cfe6970ce95cbeb1739bef6a43626783d58f56c224cfb606c360301f632a198db63f599fca7be2e0c2566", []uint{0, 1, 2, 3, 5})
			check(object.MatchStringNotEqual, "f3b6eedc3f30b99309582a7e0ca09dd6a9234ce95bfa578ddfa6ef2a0fe9c56e8f6a86c82ce565d9216c02110c0fe44079a68275243ad2f9be6bf7dacdeed97c", []uint{0, 1, 2, 3, 4})
			check(object.MatchStringEqual, "f3b6eedc3f30b99309582a7e0ca09dd6a9234ce95bfa578ddfa6ef2a0fe9c56e8f6a86c82ce565d9216c02110c0fe44079a68275243ad2f9be6bf7dacdeed97d", nil) // other
			check(object.MatchStringNotEqual, "f3b6eedc3f30b99309582a7e0ca09dd6a9234ce95bfa578ddfa6ef2a0fe9c56e8f6a86c82ce565d9216c02110c0fe44079a68275243ad2f9be6bf7dacdeed97d", all)
			for _, m := range []object.SearchMatchType{
				object.MatchNotPresent, object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check(m, "a73a37d54475df580b324d70f3d1ac922200af91f196dd9cb0f8f1cca5fefdf0cb3dbc4aaac639416e3fdd4c540e616e6b44ac6b56a3b194e8011925192a8be2", nil)
			}
			check(object.MatchCommonPrefix, "", all)
			check(object.MatchCommonPrefix, "a7", []uint{0})
			check(object.MatchCommonPrefix, "f3", []uint{5})
			check(object.MatchCommonPrefix, "f3b6eedc3f30b99309582a7e0ca09dd6a9234ce95bfa578ddfa6ef2a0fe9c56e8f6a86c82ce565d9216c02110c0fe44079a68275243ad2f9be6bf7dacdeed97c", []uint{5})
			check(object.MatchCommonPrefix, "f3b6eedc3f30b99309582a7e0ca09dd6a9234ce95bfa578ddfa6ef2a0fe9c56e8f6a86c82ce565d9216c02110c0fe44079a68275243ad2f9be6bf7dacdeed97d", nil)
		})
		t.Run("first ID", func(t *testing.T) {
			check := func(m object.SearchMatchType, v string, matchInds []uint) {
				check(t, "$Object:split.first", m, v, matchInds)
			}
			group1, group2, all := []uint{2, 4}, []uint{3, 5}, []uint{2, 3, 4, 5}
			check(object.MatchStringEqual, "61hnJaKip8c1QxvC2iT4Txfpxf37QBNRaw1XCeq72DbC", group1)
			check(object.MatchStringEqual, "Cdf8vnK5xTxmkdc1GcjkxaEQFtEmwHPRky4KRQik6rQH", group2)
			check(object.MatchStringNotEqual, "61hnJaKip8c1QxvC2iT4Txfpxf37QBNRaw1XCeq72DbC", group2)
			check(object.MatchStringNotEqual, "Cdf8vnK5xTxmkdc1GcjkxaEQFtEmwHPRky4KRQik6rQH", group1)
			check(object.MatchStringEqual, "Dfot9FnhkJy9m8pXrF1fL5fmKmbHK8wL8PqExoQFNTrz", nil) // other
			check(object.MatchStringNotEqual, "Dfot9FnhkJy9m8pXrF1fL5fmKmbHK8wL8PqExoQFNTrz", all)
			for _, m := range []object.SearchMatchType{
				object.MatchNotPresent, object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check(m, "61hnJaKip8c1QxvC2iT4Txfpxf37QBNRaw1XCeq72DbC", nil)
			}
			check(object.MatchCommonPrefix, "6", group1)
			check(object.MatchCommonPrefix, "C", group2)
			check(object.MatchCommonPrefix, "Cdf8vnK5xTxmkdc1GcjkxaEQFtEmwHPRky4KRQik6rQH", group2)
			check(object.MatchCommonPrefix, "Cdf8vnK5xTxmkdc1GcjkxaEQFtEmwHPRky4KRQik6rQH1", nil)
		})
		t.Run("parent ID", func(t *testing.T) {
			check := func(m object.SearchMatchType, v string, matchInds []uint) {
				check(t, "$Object:split.parent", m, v, matchInds)
			}
			group1, group2, all := []uint{2, 4}, []uint{3, 5}, []uint{2, 3, 4, 5}
			check(object.MatchStringEqual, "RSYscGLzKw1nkeVRGpowYTGgtgodXJrMyyiHTGGJW3S", group1)
			check(object.MatchStringEqual, "6dMvfyLF7HZ1WsBRgrLUDZP4pLkvNRjB6HWGeNXP4fJp", group2)
			check(object.MatchStringNotEqual, "RSYscGLzKw1nkeVRGpowYTGgtgodXJrMyyiHTGGJW3S", group2)
			check(object.MatchStringNotEqual, "6dMvfyLF7HZ1WsBRgrLUDZP4pLkvNRjB6HWGeNXP4fJp", group1)
			check(object.MatchStringEqual, "Dfot9FnhkJy9m8pXrF1fL5fmKmbHK8wL8PqExoQFNTrz", nil) // other
			check(object.MatchStringNotEqual, "Dfot9FnhkJy9m8pXrF1fL5fmKmbHK8wL8PqExoQFNTrz", all)
			for _, m := range []object.SearchMatchType{
				object.MatchNotPresent, object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check(m, "RSYscGLzKw1nkeVRGpowYTGgtgodXJrMyyiHTGGJW3S", nil)
			}
			check(object.MatchCommonPrefix, "6dMvfyLF7HZ1WsBRgrLUDZP4pLkvNRjB6HWGeNXP4fJp", group2)
			check(object.MatchCommonPrefix, "6dMvfyLF7HZ1WsBRgrLUDZP4pLkvNRjB6HWGeNXP4fJJ", nil)
		})
		t.Run("integers", func(t *testing.T) {
			allInt := []uint{0, 1, 2, 3, 4}
			for _, matcher := range []object.SearchMatchType{
				object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check(t, "global_non_integer", matcher, "123", nil)
				// TODO: also check that BoltDB is untouched in following cases
				check(t, "attr_int", matcher, "text", nil)
				check(t, "attr_int", matcher, "1.5", nil)
			}
			check(t, "attr_int", object.MatchNumLT, "-115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check(t, "attr_int", object.MatchNumLE, "-115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check(t, "attr_int", object.MatchNumGT, "-115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check(t, "attr_int", object.MatchNumGE, "-115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check(t, "attr_int", object.MatchNumLT, "115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check(t, "attr_int", object.MatchNumLE, "115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check(t, "attr_int", object.MatchNumGT, "115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check(t, "attr_int", object.MatchNumGE, "115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check(t, "attr_int", object.MatchNumLT, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", nil)
			check(t, "attr_int", object.MatchNumLT, "-18446744073709551615", []uint{0})
			check(t, "attr_int", object.MatchNumLT, "0", []uint{0, 2})
			check(t, "attr_int", object.MatchNumLT, "18446744073709551615", []uint{0, 2, 3})
			check(t, "attr_int", object.MatchNumLT, "115792089237316195423570985008687907853269984665640564039457584007913129639935", []uint{0, 2, 3, 4})
			check(t, "attr_int", object.MatchNumLE, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", []uint{0})
			check(t, "attr_int", object.MatchNumLE, "-18446744073709551615", []uint{0, 2})
			check(t, "attr_int", object.MatchNumLE, "0", []uint{0, 2, 3})
			check(t, "attr_int", object.MatchNumLE, "18446744073709551615", []uint{0, 2, 3, 4})
			check(t, "attr_int", object.MatchNumLE, "115792089237316195423570985008687907853269984665640564039457584007913129639935", allInt)
			check(t, "attr_int", object.MatchNumGT, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", []uint{1, 2, 3, 4})
			check(t, "attr_int", object.MatchNumGT, "-18446744073709551615", []uint{1, 3, 4})
			check(t, "attr_int", object.MatchNumGT, "0", []uint{1, 4})
			check(t, "attr_int", object.MatchNumGT, "18446744073709551615", []uint{1})
			check(t, "attr_int", object.MatchNumGT, "115792089237316195423570985008687907853269984665640564039457584007913129639935", nil)
			check(t, "attr_int", object.MatchNumGE, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", allInt)
			check(t, "attr_int", object.MatchNumGE, "-18446744073709551615", []uint{1, 2, 3, 4})
			check(t, "attr_int", object.MatchNumGE, "0", []uint{1, 3, 4})
			check(t, "attr_int", object.MatchNumGE, "18446744073709551615", []uint{1, 4})
			check(t, "attr_int", object.MatchNumGE, "115792089237316195423570985008687907853269984665640564039457584007913129639935", []uint{1})
			for _, tc := range []struct {
				name, key  string
				val1, val2 string
			}{
				{name: "creation epoch", key: "$Object:creationEpoch", val1: "10", val2: "11"},
				{name: "payload length", key: "$Object:payloadLength", val1: "20", val2: "21"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					check(t, tc.key, object.MatchNumLT, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", nil)
					check(t, tc.key, object.MatchNumLT, "0", nil)
					check(t, tc.key, object.MatchNumLT, tc.val1, nil)
					check(t, tc.key, object.MatchNumLT, tc.val2, group1)
					check(t, tc.key, object.MatchNumLT, "18446744073709551615", all)
					check(t, tc.key, object.MatchNumLT, "115792089237316195423570985008687907853269984665640564039457584007913129639935", all)
					check(t, tc.key, object.MatchNumLE, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", nil)
					check(t, tc.key, object.MatchNumLE, "0", nil)
					check(t, tc.key, object.MatchNumLE, tc.val1, group1)
					check(t, tc.key, object.MatchNumLE, tc.val2, all)
					check(t, tc.key, object.MatchNumLE, "18446744073709551615", all)
					check(t, tc.key, object.MatchNumLE, "115792089237316195423570985008687907853269984665640564039457584007913129639935", all)
					check(t, tc.key, object.MatchNumGT, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", all)
					check(t, tc.key, object.MatchNumGT, "0", all)
					check(t, tc.key, object.MatchNumGT, tc.val1, group2)
					check(t, tc.key, object.MatchNumGT, tc.val2, nil)
					check(t, tc.key, object.MatchNumGT, "18446744073709551615", nil)
					check(t, tc.key, object.MatchNumGT, "115792089237316195423570985008687907853269984665640564039457584007913129639935", nil)
					check(t, tc.key, object.MatchNumGE, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", all)
					check(t, tc.key, object.MatchNumGE, "0", all)
					check(t, tc.key, object.MatchNumGE, tc.val1, all)
					check(t, tc.key, object.MatchNumGE, tc.val2, group2)
					check(t, tc.key, object.MatchNumGE, "18446744073709551615", nil)
					check(t, tc.key, object.MatchNumGE, "115792089237316195423570985008687907853269984665640564039457584007913129639935", nil)
				})
			}
			t.Run("mixed", func(t *testing.T) {
				// this test cover cases when same attribute may appear as both int and non-int
				const attr = "IntMixed"
				vals := []string{"-11", "-1a", "0", "11", "11a", "12", "no digits", "o"}
				ids := sortObjectIDs(oidtest.IDs(len(vals)))
				slices.Reverse(ids)
				objs := make([]object.Object, len(vals))
				for i := range objs {
					appendAttribute(&objs[i], attr, vals[i])
				}
				// store
				cnr := cidtest.ID()
				for i := range objs {
					objs[i].SetContainerID(cnr)
					objs[i].SetID(ids[i])
					objs[i].SetOwner(usertest.ID())                                                  // Put requires
					objs[i].SetPayloadChecksum(checksum.NewSHA256(sha256.Sum256(objs[i].Payload()))) // Put requires
					require.NoError(t, db.Put(&objs[i]))
				}
				fullRes := searchResultForIDs(ids)
				for i := range vals {
					fullRes[i].Attributes = []string{vals[i]}
				}
				check := func(t *testing.T, m object.SearchMatchType, val string, inds ...uint) {
					var fs object.SearchFilters
					fs.AddFilter(attr, val, m)
					assertSearchResultIndexes(t, db, cnr, fs, []string{attr}, fullRes, inds)
				}
				all := make([]uint, len(vals))
				for i := range vals {
					all[i] = uint(i)
				}
				t.Run("EQ", func(t *testing.T) {
					for i := range vals {
						check(t, object.MatchStringEqual, vals[i], uint(i))
					}
				})
				t.Run("NE", func(t *testing.T) {
					for i := range vals {
						others := make([]uint, 0, len(vals)-1)
						for j := range vals {
							if j != i {
								others = append(others, uint(j))
							}
						}
						check(t, object.MatchStringNotEqual, vals[i], others...)
					}
					t.Run("empty", func(t *testing.T) {
						check(t, object.MatchStringNotEqual, "", all...)
					})
				})
				t.Run("PREFIX", func(t *testing.T) {
					t.Run("negative", func(t *testing.T) {
						check := func(t *testing.T, val string) {
							check(t, object.MatchCommonPrefix, val, 0, 1)
						}
						t.Run("no digits", func(t *testing.T) { check(t, "-") })
						t.Run("with digit", func(t *testing.T) { check(t, "-1") })
					})
					t.Run("positive", func(t *testing.T) {
						check(t, object.MatchCommonPrefix, "1", 3, 4, 5)
						check(t, object.MatchCommonPrefix, "11", 3, 4)
					})
					t.Run("empty", func(t *testing.T) {
						check(t, object.MatchCommonPrefix, "", all...)
					})
				})
				t.Run("NUM", func(t *testing.T) {
					t.Run("GT", func(t *testing.T) {
						check(t, object.MatchNumGT, "-12", 0, 2, 3, 5)
						check(t, object.MatchNumGT, "-11", 2, 3, 5)
					})
					t.Run("GE", func(t *testing.T) {
						check(t, object.MatchNumGE, "-11", 0, 2, 3, 5)
						check(t, object.MatchNumGE, "-10", 2, 3, 5)
					})
					t.Run("LT", func(t *testing.T) {
						check(t, object.MatchNumLT, "13", 0, 2, 3, 5)
						check(t, object.MatchNumLT, "12", 0, 2, 3)
					})
					t.Run("LE", func(t *testing.T) {
						check(t, object.MatchNumLE, "12", 0, 2, 3, 5)
						check(t, object.MatchNumLE, "11", 0, 2, 3)
					})
				})
			})
		})
		t.Run("complex", func(t *testing.T) {
			type (
				filter struct {
					k string
					m object.SearchMatchType
					v string
				}
				testCase struct {
					is []uint
					fs []filter
				}
			)
			cases := []testCase{
				{is: group1, fs: []filter{
					{k: "group_attr_1", m: object.MatchStringEqual, v: "group_val_1"},
					{k: "attr_int", m: object.MatchNumGE, v: "-115792089237316195423570985008687907853269984665640564039457584007913129639935"},
				}},
				{is: nil, fs: []filter{
					{k: "group_attr_2", m: object.MatchStringNotEqual, v: "group_val_1"},
					{k: "attr_int", m: object.MatchNumLT, v: "115792089237316195423570985008687907853269984665640564039457584007913129639936"},
				}},
				{is: nil, fs: []filter{
					{k: "attr_common", m: object.MatchCommonPrefix, v: "val_c"},
					{k: "attr_int", m: object.MatchNumLT, v: "0"},
					{k: "attr_int", m: object.MatchNumGT, v: "0"},
				}},
				{is: []uint{0, 1, 2, 3, 4}, fs: []filter{
					{k: "attr_common", m: object.MatchStringEqual, v: "val_common"},
					{k: "attr_int", m: object.MatchNumGE, v: "-115792089237316195423570985008687907853269984665640564039457584007913129639935"},
					{k: "attr_int", m: object.MatchNumLE, v: "115792089237316195423570985008687907853269984665640564039457584007913129639935"},
				}},
				{is: []uint{0, 2, 3, 4, 5}, fs: []filter{
					{k: "unique_attr_1", m: object.MatchNotPresent},
					{k: "attr_common", m: object.MatchStringNotEqual, v: "wrong text"},
				}},
				{is: []uint{0, 2, 3, 4, 5}, fs: []filter{
					{k: "unique_attr_1", m: object.MatchNotPresent},
					{k: "attr_common", m: object.MatchStringNotEqual, v: "wrong text"},
				}},
				{is: []uint{4}, fs: []filter{
					{k: "attr_int", m: object.MatchNumGT, v: "-18446744073709551615"},
					{k: "group_attr_1", m: object.MatchStringNotEqual, v: "random"},
					{k: "global_non_integer", m: object.MatchCommonPrefix, v: "not"},
					{k: "random", m: object.MatchNotPresent},
					{k: "attr_int", m: object.MatchNumGE, v: "18446744073709551615"},
				}},
				{is: nil, fs: []filter{ // like previous but > instead of >=
					{k: "attr_int", m: object.MatchNumGT, v: "-18446744073709551615"},
					{k: "group_attr_1", m: object.MatchStringNotEqual, v: "random"},
					{k: "global_non_integer", m: object.MatchCommonPrefix, v: "not"},
					{k: "random", m: object.MatchNotPresent},
					{k: "attr_int", m: object.MatchNumGT, v: "18446744073709551615"},
				}},
				{is: group2, fs: []filter{
					{k: "$Object:payloadLength", m: object.MatchNumGT, v: "20"},
					{k: "$Object:creationEpoch", m: object.MatchNumLT, v: "30"},
				}},
				{is: all, fs: []filter{
					{k: "$Object:payloadLength", m: object.MatchNumGT, v: "19"},
					{k: "$Object:creationEpoch", m: object.MatchNumLE, v: "21"},
				}},
				{is: []uint{2, 4}, fs: []filter{
					{k: "$Object:split.first", m: object.MatchStringEqual, v: "61hnJaKip8c1QxvC2iT4Txfpxf37QBNRaw1XCeq72DbC"},
					{k: "$Object:split.parent", m: object.MatchStringEqual, v: "RSYscGLzKw1nkeVRGpowYTGgtgodXJrMyyiHTGGJW3S"},
				}},
				{is: []uint{3, 5}, fs: []filter{
					{k: "$Object:split.parent", m: object.MatchStringNotEqual, v: "RSYscGLzKw1nkeVRGpowYTGgtgodXJrMyyiHTGGJW3S"},
					{k: "$Object:split.first", m: object.MatchStringEqual, v: "Cdf8vnK5xTxmkdc1GcjkxaEQFtEmwHPRky4KRQik6rQH"},
				}},
				{is: []uint{3, 5}, fs: []filter{
					{k: "random", m: object.MatchNotPresent},
					{k: "$Object:split.parent", m: object.MatchStringNotEqual, v: "RSYscGLzKw1nkeVRGpowYTGgtgodXJrMyyiHTGGJW3S"},
				}},
			}
			if testSplitID {
				cases = append(cases, testCase{is: []uint{2, 4}, fs: []filter{
					{k: "$Object:split.splitID", m: object.MatchCommonPrefix, v: "8b69e76d-5e95-4639-8213-46786c41ab73"},
					{k: "random", m: object.MatchNotPresent},
					{k: "attr_common", m: object.MatchStringNotEqual, v: "random"},
				}})
			}
			for _, tc := range cases {
				t.Run("complex", func(t *testing.T) {
					var fs object.SearchFilters
					for _, f := range tc.fs {
						fs.AddFilter(f.k, f.v, f.m)
					}
					assertSearchResultIndexes(t, db, cnr, fs, nil, searchResultForIDs(ids[:]), tc.is)
				})
			}
		})

		// not every metabase should work with deprecated split ID field
		if testSplitID {
			t.Run("split ID", func(t *testing.T) {
				check := func(m object.SearchMatchType, v string, matchInds []uint) {
					check(t, "$Object:split.splitID", m, v, matchInds)
				}
				group1, group2, all := []uint{2, 4}, []uint{3, 5}, []uint{2, 3, 4, 5}
				check(object.MatchStringEqual, "8b69e76d-5e95-4639-8213-46786c41ab73", group1)
				check(object.MatchStringNotEqual, "8b69e76d-5e95-4639-8213-46786c41ab73", group2)
				check(object.MatchStringEqual, "60c6b1ff-5e6d-4c0f-8699-15d54bf8a2e1", group2)
				check(object.MatchStringNotEqual, "60c6b1ff-5e6d-4c0f-8699-15d54bf8a2e1", group1)
				check(object.MatchStringEqual, "2a6346f2-97de-4c8d-91bf-20145cf302d6", nil) // other
				check(object.MatchStringNotEqual, "2a6346f2-97de-4c8d-91bf-20145cf302d6", all)
				for _, m := range []object.SearchMatchType{
					object.MatchNotPresent, object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
				} {
					check(m, "60c6b1ff-5e6d-4c0f-8699-15d54bf8a2e1", nil)
				}
				check(object.MatchCommonPrefix, "8b69e76d-5e95-4639-8213-46786c41ab73", group1)
				check(object.MatchCommonPrefix, "8b69e76d-5e95-4639-8213-46786c41ab74", nil)
			})
		}
	})
	t.Run("attributes", func(t *testing.T) {
		t.Run("range over integer attribute", func(t *testing.T) {
			// Similar scenario is used by NeoGo block fetcher storing blocks in the NeoFS.
			// Note that the test does not copy the approach to constructing objects, only
			// imitates.
			//
			// Let node store objects corresponding to the block with heights: 0, 1, 50,
			// 100-102, 150, 4294967295 (max). Block#101 is presented thrice (undesired but
			// possible). Additional integer attribute is also presented for testing.
			const heightAttr = "Height"
			const otherAttr = "SomeHash"
			otherAttrs := []string{ // sorted
				"0de1d9f050abdfb0fd8a4ff061aaa305dbbc63bf03d0ae2b8c93fbb8954b0201",
				"13912c19601cc2aa2c35347bc734c469907bcebe5f81812de77a4cc192f3892c",
				"1c3cd7853cfb53a134101205b6d894355ccb02ad454ac33a5ced5771a6f6dd14",
				"1c80940a5099c05680035f3fcfee6a1dc36335622428bcf40635bf86a75d512b",
				"1db039d30914eacfdf71780961e4957d512cfae597969c892ed1b59d258968e8",
				"80dbfec78f2d4b5128d8fe51c95f3bc42be741832c77c127d53ab32f4f341505",
				"9811b76c7a2b6b0020c30d4a9895fad8f2edab60037139e2a2b01761e137fb1a",
				"b7d56b41e13a4502dca18420816bb1ba1a0bc10644c5e3f2bc5c511026df5aef",
				"d6c059ae6852e04826419b0381690a1d76906721f195644863931f32f2d23842",
				"d6dc85d4c2bab1bbd6da3ebcd4c2c56f12c5c369b685cc301e9b61449abe390b",
			}
			ids := []oid.ID{ // sorted
				{5, 254, 154, 170, 83, 237, 109, 56, 68, 68, 97, 248, 50, 161, 183, 217, 28, 94, 162, 37, 79, 45, 175, 120, 104, 7, 87, 127, 92, 17, 218, 117},
				{41, 204, 35, 189, 128, 42, 229, 31, 7, 157, 117, 193, 98, 150, 30, 172, 103, 253, 100, 69, 223, 91, 232, 120, 70, 86, 242, 110, 88, 161, 62, 182},
				{54, 88, 178, 234, 172, 94, 155, 197, 69, 215, 33, 181, 122, 70, 178, 21, 158, 201, 54, 74, 21, 250, 193, 135, 123, 236, 137, 8, 81, 250, 21, 201},
				{92, 89, 108, 190, 140, 175, 71, 21, 243, 27, 88, 40, 156, 231, 102, 194, 230, 6, 109, 91, 135, 25, 190, 62, 246, 144, 137, 45, 90, 87, 186, 140},
				{116, 181, 195, 91, 211, 242, 145, 117, 174, 58, 195, 47, 208, 182, 46, 246, 18, 85, 0, 40, 129, 154, 68, 97, 225, 189, 89, 187, 194, 109, 201, 95},
				{162, 20, 218, 85, 5, 146, 98, 157, 137, 168, 59, 54, 102, 59, 86, 136, 160, 217, 143, 195, 200, 186, 192, 175, 235, 211, 101, 210, 147, 14, 141, 162},
				{178, 29, 204, 231, 34, 173, 251, 163, 135, 160, 94, 96, 171, 183, 2, 198, 53, 69, 84, 160, 76, 213, 208, 32, 247, 144, 230, 167, 70, 91, 158, 136},
				{199, 65, 97, 53, 71, 144, 40, 246, 194, 114, 139, 109, 213, 129, 253, 106, 141, 36, 249, 20, 130, 126, 245, 11, 110, 113, 50, 171, 153, 210, 119, 245},
				{237, 43, 4, 240, 144, 194, 224, 217, 7, 63, 14, 22, 147, 70, 8, 191, 226, 199, 69, 43, 131, 32, 37, 79, 151, 212, 149, 94, 172, 17, 137, 148},
				{245, 142, 55, 147, 121, 184, 29, 75, 74, 192, 85, 213, 243, 183, 80, 108, 181, 57, 119, 15, 84, 220, 143, 72, 202, 247, 28, 220, 245, 116, 128, 110},
			}
			objs := make([]object.Object, len(ids)) // 2 more objects for #101
			appendAttribute(&objs[0], heightAttr, "0")
			appendAttribute(&objs[1], heightAttr, "1")
			appendAttribute(&objs[2], heightAttr, "50")
			appendAttribute(&objs[3], heightAttr, "100")
			appendAttribute(&objs[4], heightAttr, "101")
			appendAttribute(&objs[5], heightAttr, "101")
			appendAttribute(&objs[6], heightAttr, "101")
			appendAttribute(&objs[7], heightAttr, "102")
			appendAttribute(&objs[8], heightAttr, "150")
			appendAttribute(&objs[9], heightAttr, "4294967295")
			for i := range ids {
				objs[i].SetID(ids[len(ids)-1-i]) // reverse order
			}
			for i := range otherAttrs {
				appendAttribute(&objs[i], otherAttr, otherAttrs[i])
			}
			heightSorted := []client.SearchResultItem{
				// attribute takes 1st order priority
				{ID: ids[9], Attributes: []string{"0", otherAttrs[0]}},
				{ID: ids[8], Attributes: []string{"1", otherAttrs[1]}},
				{ID: ids[7], Attributes: []string{"50", otherAttrs[2]}},
				{ID: ids[6], Attributes: []string{"100", otherAttrs[3]}},
				// but if attribute equals, items are sorted by IDs. Secondary attributes have
				// no effect, otherwise the order would not be reversed
				{ID: ids[3], Attributes: []string{"101", otherAttrs[6]}},
				{ID: ids[4], Attributes: []string{"101", otherAttrs[5]}},
				{ID: ids[5], Attributes: []string{"101", otherAttrs[4]}},
				// attribute takes power again
				{ID: ids[2], Attributes: []string{"102", otherAttrs[7]}},
				{ID: ids[1], Attributes: []string{"150", otherAttrs[8]}},
				{ID: ids[0], Attributes: []string{"4294967295", otherAttrs[9]}},
			}
			// store
			cnr := cidtest.ID()
			for i := range objs {
				objs[i].SetContainerID(cnr)
				objs[i].SetOwner(usertest.ID())                                                  // Put requires
				objs[i].SetPayloadChecksum(checksum.NewSHA256(sha256.Sum256(objs[i].Payload()))) // Put requires
				require.NoError(t, db.Put(&objs[i]))
			}
			t.Run("none", func(t *testing.T) {
				for _, set := range []func(*object.SearchFilters){
					func(fs *object.SearchFilters) { fs.AddFilter(heightAttr, "", object.MatchNotPresent) },
					func(fs *object.SearchFilters) { fs.AddFilter(otherAttr, "", object.MatchNotPresent) },
					func(fs *object.SearchFilters) { fs.AddFilter(heightAttr, "0", object.MatchNumLT) },
					func(fs *object.SearchFilters) { fs.AddFilter(heightAttr, "4294967295", object.MatchNumGT) },
					func(fs *object.SearchFilters) {
						fs.AddFilter(heightAttr, "0", object.MatchNumGE)
						fs.AddFilter(heightAttr, "151", object.MatchStringEqual)
					},
				} {
					var fs object.SearchFilters
					set(&fs)
					assertSearchResult(t, db, cnr, fs, nil, nil)
				}
			})
			t.Run("all", func(t *testing.T) {
				t.Run("unfiltered", func(t *testing.T) {
					assertSearchResult(t, db, cnr, nil, nil, searchResultForIDs(ids))
				})
				var fs object.SearchFilters
				fs.AddFilter(heightAttr, "0", object.MatchNumGE)
				t.Run("w/o attributes", func(t *testing.T) {
					assertSearchResult(t, db, cnr, fs, nil, searchResultForIDs(ids))
				})
				t.Run("single attribute", func(t *testing.T) {
					assertSearchResult(t, db, cnr, fs, []string{heightAttr}, heightSorted)
				})
				t.Run("two attributes", func(t *testing.T) {
					assertSearchResult(t, db, cnr, fs, []string{heightAttr, otherAttr}, heightSorted)
				})
			})
			t.Run("partial", func(t *testing.T) {
				var fs object.SearchFilters
				fs.AddFilter(heightAttr, "50", object.MatchNumGE)
				fs.AddFilter(heightAttr, "150", object.MatchNumLE)
				heightSorted := heightSorted[2:9]
				ids := ids[1:8]
				t.Run("w/o attributes", func(t *testing.T) {
					assertSearchResult(t, db, cnr, fs, nil, searchResultForIDs(ids))
				})
				t.Run("single attribute", func(t *testing.T) {
					assertSearchResult(t, db, cnr, fs, []string{heightAttr}, heightSorted)
				})
				t.Run("two attributes", func(t *testing.T) {
					assertSearchResult(t, db, cnr, fs, []string{heightAttr, otherAttr}, heightSorted)
				})
			})
		})
		t.Run("FilePath+Timestamp", func(t *testing.T) {
			// REST GW use-case
			ids := []oid.ID{ // sorted
				{5, 254, 154, 170, 83, 237, 109, 56, 68, 68, 97, 248, 50, 161, 183, 217, 28, 94, 162, 37, 79, 45, 175, 120, 104, 7, 87, 127, 92, 17, 218, 117},
				{41, 204, 35, 189, 128, 42, 229, 31, 7, 157, 117, 193, 98, 150, 30, 172, 103, 253, 100, 69, 223, 91, 232, 120, 70, 86, 242, 110, 88, 161, 62, 182},
				{54, 88, 178, 234, 172, 94, 155, 197, 69, 215, 33, 181, 122, 70, 178, 21, 158, 201, 54, 74, 21, 250, 193, 135, 123, 236, 137, 8, 81, 250, 21, 201},
				{92, 89, 108, 190, 140, 175, 71, 21, 243, 27, 88, 40, 156, 231, 102, 194, 230, 6, 109, 91, 135, 25, 190, 62, 246, 144, 137, 45, 90, 87, 186, 140},
			}
			objs := make([]object.Object, len(ids))
			appendAttribute(&objs[0], object.AttributeFilePath, "cat1.jpg")
			appendAttribute(&objs[0], object.AttributeTimestamp, "1738760790")
			appendAttribute(&objs[1], object.AttributeFilePath, "cat2.jpg")
			appendAttribute(&objs[1], object.AttributeTimestamp, "1738760792")
			appendAttribute(&objs[2], object.AttributeFilePath, "cat2.jpg")
			appendAttribute(&objs[2], object.AttributeTimestamp, "1738760791")
			appendAttribute(&objs[3], object.AttributeFilePath, "cat2.jpg")
			appendAttribute(&objs[3], object.AttributeTimestamp, "1738760793")
			// store
			cnr := cidtest.ID()
			for i := range objs {
				objs[i].SetID(ids[i])
				objs[i].SetContainerID(cnr)
				objs[i].SetOwner(usertest.ID())                                                  // Put requires
				objs[i].SetPayloadChecksum(checksum.NewSHA256(sha256.Sum256(objs[i].Payload()))) // Put requires
				require.NoError(t, db.Put(&objs[i]))
			}
			t.Run("none", func(t *testing.T) {
				var fs object.SearchFilters
				fs.AddFilter(object.AttributeFilePath, "cat4.jpg", object.MatchStringEqual)
				assertSearchResult(t, db, cnr, fs, []string{object.AttributeFilePath, object.AttributeTimestamp}, nil)
			})
			t.Run("single", func(t *testing.T) {
				var fs object.SearchFilters
				fs.AddFilter(object.AttributeFilePath, "cat1.jpg", object.MatchStringEqual)
				assertSearchResult(t, db, cnr, fs, []string{object.AttributeFilePath, object.AttributeTimestamp}, []client.SearchResultItem{
					{ID: ids[0], Attributes: []string{"cat1.jpg", "1738760790"}},
				})
			})
			t.Run("multiple", func(t *testing.T) {
				t.Run("both attributes", func(t *testing.T) {
					fullRes := []client.SearchResultItem{
						{ID: ids[1], Attributes: []string{"cat2.jpg", "1738760792"}},
						{ID: ids[2], Attributes: []string{"cat2.jpg", "1738760791"}},
						{ID: ids[3], Attributes: []string{"cat2.jpg", "1738760793"}},
					}
					var fs object.SearchFilters
					fs.AddFilter(object.AttributeFilePath, "cat2.jpg", object.MatchStringEqual)
					assertSearchResult(t, db, cnr, fs, []string{object.AttributeFilePath, object.AttributeTimestamp}, fullRes)
				})
			})
		})
		t.Run("precise select with many attributes", func(t *testing.T) {
			// S3 GW use-case
			ids := []oid.ID{ // sorted
				{5, 254, 154, 170, 83, 237, 109, 56, 68, 68, 97, 248, 50, 161, 183, 217, 28, 94, 162, 37, 79, 45, 175, 120, 104, 7, 87, 127, 92, 17, 218, 117},
				{41, 204, 35, 189, 128, 42, 229, 31, 7, 157, 117, 193, 98, 150, 30, 172, 103, 253, 100, 69, 223, 91, 232, 120, 70, 86, 242, 110, 88, 161, 62, 182},
				{54, 88, 178, 234, 172, 94, 155, 197, 69, 215, 33, 181, 122, 70, 178, 21, 158, 201, 54, 74, 21, 250, 193, 135, 123, 236, 137, 8, 81, 250, 21, 201},
				{92, 89, 108, 190, 140, 175, 71, 21, 243, 27, 88, 40, 156, 231, 102, 194, 230, 6, 109, 91, 135, 25, 190, 62, 246, 144, 137, 45, 90, 87, 186, 140},
			}
			objs := make([]object.Object, len(ids))
			appendAttribute(&objs[0], object.AttributeFilePath, "/home/Downloads/dog.jpg")
			appendAttribute(&objs[0], "Type", "JPEG")
			appendAttribute(&objs[0], "attr1", "val1_1")
			appendAttribute(&objs[0], "attr2", "val2_1")
			appendAttribute(&objs[1], object.AttributeFilePath, "/usr/local/bin/go")
			appendAttribute(&objs[1], "Type", "BIN")
			appendAttribute(&objs[1], "attr1", "val1_2")
			appendAttribute(&objs[1], "attr2", "val2_2")
			appendAttribute(&objs[2], object.AttributeFilePath, "/home/Downloads/cat.jpg")
			appendAttribute(&objs[2], "Type", "JPEG")
			appendAttribute(&objs[2], "attr1", "val1_3")
			appendAttribute(&objs[2], "attr2", "val2_3")
			appendAttribute(&objs[3], object.AttributeFilePath, "/var/log/neofs/node")
			appendAttribute(&objs[3], "Type", "TEXT")
			appendAttribute(&objs[3], "attr1", "val1_4")
			appendAttribute(&objs[3], "attr2", "val2_4")
			// store
			cnr := cidtest.ID()
			for i := range objs {
				objs[i].SetID(ids[len(ids)-i-1])
				objs[i].SetContainerID(cnr)
				objs[i].SetOwner(usertest.ID())                                                  // Put requires
				objs[i].SetPayloadChecksum(checksum.NewSHA256(sha256.Sum256(objs[i].Payload()))) // Put requires
				require.NoError(t, db.Put(&objs[i]))
			}

			attrs := []string{object.AttributeFilePath, "attr1", "attr2"}

			var fs object.SearchFilters
			fs.AddFilter(object.AttributeFilePath, "/home/Downloads/", object.MatchCommonPrefix)
			fs.AddFilter("Type", "JPEG", object.MatchStringEqual)
			assertSearchResult(t, db, cnr, fs, attrs, []client.SearchResultItem{
				{ID: ids[1], Attributes: []string{"/home/Downloads/cat.jpg", "val1_3", "val2_3"}},
				{ID: ids[3], Attributes: []string{"/home/Downloads/dog.jpg", "val1_1", "val2_1"}},
			})

			fs = fs[:0]
			fs.AddFilter(object.AttributeFilePath, "/usr", object.MatchCommonPrefix)
			fs.AddFilter("Type", "BIN", object.MatchStringEqual)
			assertSearchResult(t, db, cnr, fs, attrs, []client.SearchResultItem{
				{ID: ids[2], Attributes: []string{"/usr/local/bin/go", "val1_2", "val2_2"}},
			})

			fs = fs[:0]
			fs.AddFilter(object.AttributeFilePath, "/", object.MatchCommonPrefix)
			fs.AddFilter("Type", "BIN", object.MatchStringNotEqual)
			assertSearchResult(t, db, cnr, fs, attrs, []client.SearchResultItem{
				{ID: ids[1], Attributes: []string{"/home/Downloads/cat.jpg", "val1_3", "val2_3"}},
				{ID: ids[3], Attributes: []string{"/home/Downloads/dog.jpg", "val1_1", "val2_1"}},
				{ID: ids[0], Attributes: []string{"/var/log/neofs/node", "val1_4", "val2_4"}},
			})

			fs = fs[:0]
			fs.AddFilter(object.AttributeFilePath, "/home/", object.MatchCommonPrefix)
			fs.AddFilter("Type", "TEXT", object.MatchStringNotEqual)
			assertSearchResult(t, db, cnr, fs, attrs, []client.SearchResultItem{
				{ID: ids[1], Attributes: []string{"/home/Downloads/cat.jpg", "val1_3", "val2_3"}},
				{ID: ids[3], Attributes: []string{"/home/Downloads/dog.jpg", "val1_1", "val2_1"}},
			})
		})

		id := oidtest.ID()
		ver := version.New(123, 456)
		uid := uuid.UUID{143, 198, 73, 4, 7, 21, 74, 215, 163, 144, 59, 117, 62, 93, 110, 163}
		var splitID object.SplitID
		splitID.SetUUID(uid)
		const userAttr1, userAttrVal1 = "hello", "world"
		const userAttr2, userAttrVal2 = "foo", "bar"

		var obj object.Object
		obj.SetID(id)
		obj.SetVersion(&ver)
		obj.SetCreationEpoch(720095763387318213)
		obj.SetPayloadSize(7974916746359921405)
		obj.SetOwner(user.ID{53, 128, 35, 128, 69, 168, 68, 161, 126, 161, 182, 128, 85, 91, 199, 49, 100, 216, 200, 164, 17, 127, 44, 123, 211})
		obj.SetType(object.TypeTombstone)
		obj.SetPayloadChecksum(checksum.NewSHA256([sha256.Size]byte{105, 23, 175, 222, 242, 223, 82, 69, 207, 193, 106,
			168, 9, 238, 85, 29, 34, 68, 233, 54, 143, 217, 223, 248, 236, 227, 121, 195, 155, 187, 37, 242}))
		obj.SetPayloadHomomorphicHash(checksum.NewTillichZemor([tz.Size]byte{171, 152, 81, 127, 134, 240, 228, 236, 10, 131,
			10, 114, 174, 138, 120, 108, 165, 104, 36, 100, 129, 235, 160, 213, 96, 230, 190, 15, 196, 5, 252, 194, 205, 48,
			236, 57, 117, 238, 170, 36, 251, 104, 62, 124, 1, 206, 131, 226, 221, 111, 73, 54, 235, 100, 49, 32, 252, 255,
			92, 51, 30, 77, 180, 53}))
		obj.SetParentID(oid.ID{146, 29, 179, 9, 47, 100, 26, 60, 219, 24, 253, 162, 255, 167, 39, 143, 234, 249, 77, 247, 52, 61, 3, 239, 110, 167, 61, 199, 138, 223, 198, 245})
		obj.SetFirstID(oid.ID{35, 78, 81, 228, 188, 71, 53, 15, 64, 54, 230, 84, 94, 176, 193, 118, 225, 186, 208, 33, 175, 155, 154, 205, 116, 5, 247, 138, 65, 155, 210, 145})
		obj.SetSplitID(&splitID)
		appendAttribute(&obj, userAttr1, userAttrVal1)
		appendAttribute(&obj, userAttr2, userAttrVal2)

		cases := []struct{ name, attr, val string }{
			{name: "version", attr: "$Object:version", val: "v123.456"},
			{name: "owner", attr: "$Object:ownerID", val: "NXbWCy4NfKve5AXQRh8ZnKuzo4nHmNxbPg"},
			{name: "creation epoch", attr: "$Object:creationEpoch", val: "720095763387318213"},
			{name: "payload length", attr: "$Object:payloadLength", val: "7974916746359921405"},
			{name: "type", attr: "$Object:objectType", val: "TOMBSTONE"},
			{name: "payload hash", attr: "$Object:payloadHash", val: "6917afdef2df5245cfc16aa809ee551d2244e9368fd9dff8ece379c39bbb25f2"},
			{name: "payload homomorphic hash", attr: "$Object:homomorphicHash", val: "ab98517f86f0e4ec0a830a72ae8a786ca568246481eba0d560e6be0fc405fcc2cd30ec3975eeaa24fb683e7c01ce83e2dd6f4936eb643120fcff5c331e4db435"},
			{name: "split-parent", attr: "$Object:split.parent", val: "AqNnq29xxX33yEBX8XMtcwMyCCqD876BSjHsvroTUK5n"},
			{name: "split-first", attr: "$Object:split.first", val: "3NpY5tNeZEBBma7TpesPCaT3uDNKPtvgdELXsuf8uMeG"},
			{name: "user-defined", attr: userAttr2, val: userAttrVal2},
		}
		if testSplitID {
			cases = append(cases, struct{ name, attr, val string }{name: "split-id", attr: "$Object:split.splitID", val: "8fc64904-0715-4ad7-a390-3b753e5d6ea3"})
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				cnr := cidtest.ID()
				obj.SetContainerID(cnr)
				require.NoError(t, db.Put(&obj))
				t.Run("primary", func(t *testing.T) {
					var fs object.SearchFilters
					fs.AddFilter(tc.attr, tc.val, object.MatchStringEqual)
					fs.AddFilter(userAttr1, userAttrVal1, object.MatchStringEqual)
					assertSearchResult(t, db, cnr, fs, []string{tc.attr}, []client.SearchResultItem{{ID: id, Attributes: []string{tc.val}}})
				})
				t.Run("secondary", func(t *testing.T) {
					var fs object.SearchFilters
					fs.AddFilter(userAttr1, userAttrVal1, object.MatchStringEqual)
					fs.AddFilter(tc.attr, tc.val, object.MatchStringEqual)
					assertSearchResult(t, db, cnr, fs, []string{userAttr1, tc.attr}, []client.SearchResultItem{{ID: id, Attributes: []string{userAttrVal1, tc.val}}})
				})
			})
		}
	})
}

func sortObjectIDs(ids []oid.ID) []oid.ID {
	s := slices.Clone(ids)
	slices.SortFunc(s, func(a, b oid.ID) int { return bytes.Compare(a[:], b[:]) })
	return s
}

func appendAttribute(obj *object.Object, k, v string) {
	obj.SetAttributes(append(obj.Attributes(), object.NewAttribute(k, v))...)
}

func assertSearchResultIndexes(t *testing.T, db DB, cnr cid.ID, fs object.SearchFilters, attrs []string, all []client.SearchResultItem, inds []uint) {
	require.True(t, slices.IsSorted(inds))
	expRes := make([]client.SearchResultItem, len(inds))
	for i := range inds {
		expRes[i] = all[inds[i]]
	}
	assertSearchResult(t, db, cnr, fs, attrs, expRes)
}

func assertSearchResult(t *testing.T, db DB, cnr cid.ID, fs object.SearchFilters, attrs []string, expRes []client.SearchResultItem) {
	for _, lim := range []uint16{1000, 1, 2} {
		t.Run(fmt.Sprintf("limit=%d", lim), func(t *testing.T) {
			assertSearchResultWithLimit(t, db, cnr, fs, attrs, expRes, lim)
		})
	}
}

func assertSearchResultWithLimit(t testing.TB, db DB, cnr cid.ID, fs object.SearchFilters, attrs []string, expRes []client.SearchResultItem, lim uint16) {
	_assertSearchResultWithLimit(t, db, cnr, fs, attrs, expRes, lim)
	if len(attrs) > 0 {
		expRes = slices.Clone(expRes)
		slices.SortFunc(expRes, func(a, b client.SearchResultItem) int { return bytes.Compare(a.ID[:], b.ID[:]) })
		_assertSearchResultWithLimit(t, db, cnr, fs, nil, expRes, lim)
	}
}

func cloneSearchCursor(c *objectcore.SearchCursor) *objectcore.SearchCursor {
	if c == nil {
		return nil
	}
	return &objectcore.SearchCursor{PrimaryKeysPrefix: slices.Clone(c.PrimaryKeysPrefix), PrimarySeekKey: slices.Clone(c.PrimarySeekKey)}
}

func cloneFilters(src []objectcore.SearchFilter) []objectcore.SearchFilter {
	if src == nil {
		return nil
	}
	dst := slices.Clone(src)
	for k, f := range src {
		var n *big.Int
		if f.Parsed != nil {
			n = new(big.Int).Set(f.Parsed)
		}
		dst[k].Parsed = n
		dst[k].Raw = slices.Clone(f.Raw)
	}
	return dst
}

func searchResultForIDs(ids []oid.ID) []client.SearchResultItem {
	res := make([]client.SearchResultItem, len(ids))
	for i := range ids {
		res[i].ID = ids[i]
	}
	return res
}

func _assertSearchResultWithLimit(t testing.TB, db DB, cnr cid.ID, fs object.SearchFilters, attrs []string, all []client.SearchResultItem, lim uint16) {
	var strCursor string
	nAttr := len(attrs)
	for {
		ofs, cursor, err := objectcore.PreprocessSearchQuery(fs, attrs, strCursor)
		if err != nil {
			if len(all) == 0 {
				require.ErrorIs(t, err, objectcore.ErrUnreachableQuery)
			} else {
				require.NoError(t, err)
			}
			return
		}

		cursorClone := cloneSearchCursor(cursor)
		ofsClone := cloneFilters(ofs)

		res, c, err := db.Search(cnr, ofsClone, attrs, cursor, lim)
		require.Equal(t, cursorClone, cursor, "cursor mutation detected", "cursor: %q", strCursor)
		require.Equal(t, ofsClone, ofs, "filter slice mutation detected", "cursor: %q", strCursor)
		require.NoError(t, err, "cursor: %q", strCursor)

		n := min(len(all), int(lim))
		require.Len(t, res, n)

		for i := range n { // all[:n] == res assert can lead to huge output when failed
			require.Equalf(t, all[i].ID, res[i].ID, "cursor: %q, i: %d", strCursor, i)
			require.Len(t, res[i].Attributes, nAttr)
			if nAttr > 0 {
				require.Equal(t, all[i].Attributes[:nAttr], res[i].Attributes)
			}
		}

		if all = all[n:]; len(all) == 0 {
			require.Nilf(t, c, "cursor: %q", strCursor)
			break
		}
		require.NotNilf(t, c, "cursor: %q", strCursor)

		var firstFilter *object.SearchFilter
		if len(fs) > 0 {
			firstFilter = &fs[0]
		}
		cc, err := objectcore.CalculateCursor(firstFilter, res[n-1])
		require.NoError(t, err, "cursor: %q", strCursor)
		require.Equal(t, c, cc, "cursor: %q", strCursor)

		strCursor = base64.StdEncoding.EncodeToString(c)
	}
}
