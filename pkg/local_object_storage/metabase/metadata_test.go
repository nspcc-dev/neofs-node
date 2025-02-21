package meta

import (
	"bytes"
	"encoding/base64"
	"math"
	"math/big"
	"math/rand"
	"slices"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	checksumtest "github.com/nspcc-dev/neofs-sdk-go/checksum/test"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func sortObjectIDs(ids []oid.ID) []oid.ID {
	s := slices.Clone(ids)
	slices.SortFunc(s, func(a, b oid.ID) int { return bytes.Compare(a[:], b[:]) })
	return s
}

func appendAttribute(obj *object.Object, k, v string) {
	obj.SetAttributes(append(obj.Attributes(), *object.NewAttribute(k, v))...)
}

func assertPrefixedAttrIDPresence[T string | []byte](t testing.TB, mb *bbolt.Bucket, id oid.ID, prefix byte, attr string, val T, exp bool) {
	k := []byte{prefix}
	k = append(k, attr...)
	k = append(k, 0xFF)
	k = append(k, val...)
	k = append(k, id[:]...)
	require.Equal(t, exp, mb.Get(k) != nil)
}

func assertAttrPresence[T string | []byte](t testing.TB, mb *bbolt.Bucket, id oid.ID, attr string, val T, exp bool) {
	assertPrefixedAttrIDPresence(t, mb, id, 0x02, attr, val, exp)
	k := []byte{0x03}
	k = append(k, id[:]...)
	k = append(k, attr...)
	k = append(k, 0xFF)
	k = append(k, val...)
	require.Equal(t, exp, mb.Get(k) != nil)
}

func assertAttr[T string | []byte](t testing.TB, mb *bbolt.Bucket, id oid.ID, attr string, val T) {
	assertAttrPresence(t, mb, id, attr, val, true)
}

func assertIntAttr(t testing.TB, mb *bbolt.Bucket, id oid.ID, attr string, origin string, val []byte) {
	assertAttr(t, mb, id, attr, origin)
	assertPrefixedAttrIDPresence(t, mb, id, 0x01, attr, val, true)
}

func TestPutMetadata(t *testing.T) {
	db := newDB(t)
	cnr := cidtest.ID()
	id := oidtest.ID()
	owner := user.ID{53, 79, 133, 229, 135, 39, 60, 187, 194, 109, 18, 37, 225, 166, 197, 146, 118, 186, 18, 215, 33, 158, 202, 214, 188}
	const creationEpoch = 7311064694303989735
	const payloadLen = 2091724451450177666
	const typ = 4 // can be any, max supported value at the moment
	var ver version.Version
	ver.SetMajor(2138538449)
	ver.SetMinor(1476143219)
	parentID := oid.ID{65, 202, 224, 1, 198, 23, 145, 189, 139, 236, 185, 132, 138, 222, 233, 224, 38, 204, 39, 52, 161, 38, 68,
		74, 8, 253, 255, 34, 110, 49, 90, 71}
	firstID := oid.ID{207, 78, 197, 150, 88, 190, 144, 92, 46, 19, 159, 238, 189, 151, 253, 57, 82, 204, 23, 108, 6, 96, 55, 223, 108,
		74, 176, 135, 29, 55, 177, 219}
	pldHashBytes := [32]byte{95, 165, 98, 74, 58, 67, 109, 195, 226, 238, 253, 241, 64, 7, 241, 240, 241, 46, 243, 182, 130, 17, 194,
		11, 7, 153, 171, 79, 131, 76, 154, 91}
	pldHash := checksum.NewSHA256(pldHashBytes)
	pldHmmHashBytes := [64]byte{124, 127, 67, 236, 186, 166, 150, 202, 4, 115, 163, 58, 242, 73, 149, 35, 153, 93, 4, 247, 62, 18, 13, 150,
		53, 141, 131, 172, 207, 164, 187, 240, 16, 30, 18, 30, 136, 0, 197, 213, 185, 62, 153, 223, 42, 213, 207, 86, 131, 144, 121,
		127, 251, 248, 253, 176, 145, 101, 69, 75, 12, 97, 27, 19}
	pldHmmHash := checksum.NewTillichZemor(pldHmmHashBytes)
	splitID := []byte{240, 204, 35, 185, 222, 70, 69, 124, 160, 224, 208, 185, 9, 114, 37, 109}
	var attrs []object.Attribute
	addAttr := func(k, v string) { attrs = append(attrs, *object.NewAttribute(k, v)) }
	addAttr("attr_1", "val_1")
	addAttr("attr_2", "val_2")
	addAttr("num_negative_overflow", "-115792089237316195423570985008687907853269984665640564039457584007913129639936")
	addAttr("num_negative_min", "-115792089237316195423570985008687907853269984665640564039457584007913129639935")
	addAttr("num_negative_min64", "-9223372036854775808")
	addAttr("num_negative_max", "-1")
	addAttr("num_zero", "0")
	addAttr("num_positive_min", "1")
	addAttr("num_positive_max64", "18446744073709551615")
	addAttr("num_positive_max", "115792089237316195423570985008687907853269984665640564039457584007913129639935")
	addAttr("num_positive_overflow", "115792089237316195423570985008687907853269984665640564039457584007913129639936")

	var obj object.Object
	obj.SetContainerID(cnr)
	obj.SetID(id)
	obj.SetOwner(owner)
	obj.SetCreationEpoch(creationEpoch)
	obj.SetPayloadSize(payloadLen)
	obj.SetType(typ)
	obj.SetVersion(&ver)
	obj.SetParentID(parentID)
	obj.SetFirstID(firstID)
	obj.SetPayloadChecksum(pldHash)
	obj.SetPayloadHomomorphicHash(pldHmmHash)
	obj.SetSplitID(object.NewSplitIDFromV2(splitID))
	obj.SetAttributes(attrs...)

	err := db.Put(&obj, nil, nil)
	require.NoError(t, err)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		mb := tx.Bucket(append([]byte{0xFF}, cnr[:]...))
		require.NotNil(t, mb, "missing container's meta bucket")

		require.Equal(t, []byte{}, mb.Get(append([]byte{0x00}, id[:]...)))
		assertAttr(t, mb, id, "$Object:version", "v2138538449.1476143219")
		assertAttr(t, mb, id, "$Object:ownerID", owner[:])
		assertAttr(t, mb, id, "$Object:objectType", "LINK")
		assertAttr(t, mb, id, "$Object:payloadHash", pldHashBytes[:])
		assertAttr(t, mb, id, "$Object:homomorphicHash", pldHmmHashBytes[:])
		assertAttr(t, mb, id, "$Object:split.parent", parentID[:])
		assertAttr(t, mb, id, "$Object:split.first", firstID[:])
		assertIntAttr(t, mb, id, "$Object:creationEpoch", "7311064694303989735", []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 101, 118, 30, 154, 145, 227, 159, 231})
		assertIntAttr(t, mb, id, "$Object:payloadLength", "2091724451450177666", []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 29, 7, 76, 78, 96, 175, 200, 130})
		assertAttrPresence(t, mb, id, "$Object:ROOT", "1", false)
		assertAttr(t, mb, id, "$Object:PHY", "1")
		assertAttr(t, mb, id, "attr_1", "val_1")
		assertAttr(t, mb, id, "attr_2", "val_2")
		assertAttr(t, mb, id, "num_negative_overflow", "-115792089237316195423570985008687907853269984665640564039457584007913129639936")
		assertIntAttr(t, mb, id, "num_negative_min", "-115792089237316195423570985008687907853269984665640564039457584007913129639935", []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
		assertIntAttr(t, mb, id, "num_negative_min64", "-9223372036854775808", []byte{0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
			255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 127, 255, 255, 255, 255, 255, 255, 255})
		assertIntAttr(t, mb, id, "num_negative_max", "-1", []byte{0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
			255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 254})
		assertIntAttr(t, mb, id, "num_zero", "0", []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
		assertIntAttr(t, mb, id, "num_positive_max64", "18446744073709551615", []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255})
		assertIntAttr(t, mb, id, "num_positive_max", "115792089237316195423570985008687907853269984665640564039457584007913129639935", []byte{1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
			255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255})
		assertAttr(t, mb, id, "num_positive_overflow", "115792089237316195423570985008687907853269984665640564039457584007913129639936")

		return nil
	})
	require.NoError(t, err)
}

func TestApplyFilter(t *testing.T) {
	t.Run("unsupported matcher", func(t *testing.T) {
		ok := matchValues(nil, 9, nil)
		require.False(t, ok)
	})
	t.Run("not present", func(t *testing.T) {
		require.Panics(t, func() { _ = matchValues(nil, object.MatchNotPresent, nil) })
	})
	check := func(dbVal []byte, m object.SearchMatchType, fltVal []byte, exp bool) {
		ok := matchValues(dbVal, m, fltVal)
		require.Equal(t, exp, ok)
	}
	anyData := []byte("Hello, world!")
	t.Run("EQ", func(t *testing.T) {
		check := func(dbVal, fltVal []byte, exp bool) { check(dbVal, object.MatchStringEqual, fltVal, exp) }
		check(nil, nil, true)
		check([]byte{}, nil, true)
		check(anyData, anyData, true)
		check(anyData, anyData[:len(anyData)-1], false)
		check(anyData, append(anyData, 1), false)
		for i := range anyData {
			dbVal := slices.Clone(anyData)
			dbVal[i]++
			check(dbVal, anyData, false)
		}
	})
	t.Run("NE", func(t *testing.T) {
		check := func(dbVal, fltVal []byte, exp bool) { check(dbVal, object.MatchStringNotEqual, fltVal, exp) }
		check(nil, nil, false)
		check([]byte{}, nil, false)
		check(anyData, anyData, false)
		check(anyData, anyData[:len(anyData)-1], true)
		check(anyData, append(anyData, 1), true)
		for i := range anyData {
			dbVal := slices.Clone(anyData)
			dbVal[i]++
			check(dbVal, anyData, true)
		}
	})
	t.Run("has prefix", func(t *testing.T) {
		check := func(dbVal, fltVal []byte, exp bool) { check(dbVal, object.MatchCommonPrefix, fltVal, exp) }
		check(nil, nil, true)
		check([]byte{}, nil, true)
		check(anyData, anyData, true)
		check(anyData, anyData[:len(anyData)-1], true)
		check(anyData, append(anyData, 1), false)
		for i := range anyData {
			check(anyData, anyData[:i], true)
			changed := slices.Concat(anyData[:i], []byte{anyData[i] + 1}, anyData[i+1:])
			check(anyData, changed[:i+1], false)
		}
	})
	t.Run("int", func(t *testing.T) {
		check := func(dbVal *big.Int, matcher object.SearchMatchType, fltVal *big.Int, exp bool) {
			require.Equal(t, exp, intMatches(dbVal, matcher, fltVal))
			if intWithinLimits(dbVal) && intWithinLimits(fltVal) {
				require.Equal(t, exp, intBytesMatch(intBytes(dbVal), matcher, intBytes(fltVal)))
			}
		}
		one := big.NewInt(1)
		max64 := new(big.Int).SetUint64(math.MaxUint64)
		ltMin := new(big.Int).Sub(maxUint256Neg, one)
		gtMax := new(big.Int).Add(maxUint256, one)
		ns := []*big.Int{
			maxUint256Neg,
			new(big.Int).Add(maxUint256Neg, big.NewInt(1)),
			new(big.Int).Neg(max64),
			big.NewInt(-1),
			big.NewInt(0),
			one,
			max64,
			new(big.Int).Sub(maxUint256, big.NewInt(1)),
			maxUint256,
		}
		for i, n := range ns {
			check(n, object.MatchNumGT, ltMin, true)
			check(n, object.MatchNumGE, ltMin, true)

			check(n, object.MatchNumLT, gtMax, true)
			check(n, object.MatchNumLE, gtMax, true)

			check(n, object.MatchNumGT, n, false)
			check(n, object.MatchNumGE, n, true)
			check(n, object.MatchNumLT, n, false)
			check(n, object.MatchNumLE, n, true)

			for j := range i {
				check(n, object.MatchNumGT, ns[j], true)
				check(n, object.MatchNumGE, ns[j], true)
				check(n, object.MatchNumLT, ns[j], false)
				check(n, object.MatchNumLE, ns[j], false)
			}
			for j := i + 1; j < len(ns); j++ {
				check(n, object.MatchNumGT, ns[j], false)
				check(n, object.MatchNumGE, ns[j], false)
				check(n, object.MatchNumLT, ns[j], true)
				check(n, object.MatchNumLE, ns[j], true)
			}

			minusOne := new(big.Int).Sub(n, one)
			check(n, object.MatchNumGT, minusOne, true)
			check(n, object.MatchNumGE, minusOne, true)
			if minusOne.Cmp(maxUint256Neg) >= 0 {
				check(n, object.MatchNumLT, minusOne, false)
				check(n, object.MatchNumLE, minusOne, false)
			}
			plusOne := new(big.Int).Add(n, one)
			check(n, object.MatchNumLT, plusOne, true)
			check(n, object.MatchNumLE, plusOne, true)
			if plusOne.Cmp(maxUint256) <= 0 {
				check(n, object.MatchNumGT, plusOne, false)
				check(n, object.MatchNumGE, plusOne, false)
			}
		}
	})
}

func TestIntBucketOrder(t *testing.T) {
	db := newDB(t)
	ns := []*big.Int{
		maxUint256Neg,
		new(big.Int).Add(maxUint256Neg, big.NewInt(1)),
		big.NewInt(math.MinInt64),
		big.NewInt(-1),
		big.NewInt(0),
		big.NewInt(1),
		new(big.Int).SetUint64(math.MaxUint64),
		new(big.Int).Sub(maxUint256, big.NewInt(1)),
		maxUint256,
	}
	rand.Shuffle(len(ns), func(i, j int) { ns[i], ns[j] = ns[j], ns[i] })

	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("any"))
		if err != nil {
			return err
		}
		for _, n := range ns {
			if err := b.Put(intBytes(n), nil); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	var collected []string
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket([]byte("any")).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			val, err := restoreIntAttribute(k)
			require.NoError(t, err)
			collected = append(collected, val)
		}
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, []string{
		"-115792089237316195423570985008687907853269984665640564039457584007913129639935",
		"-115792089237316195423570985008687907853269984665640564039457584007913129639934",
		"-9223372036854775808",
		"-1",
		"0",
		"1",
		"18446744073709551615",
		"115792089237316195423570985008687907853269984665640564039457584007913129639934",
		"115792089237316195423570985008687907853269984665640564039457584007913129639935",
	}, collected)
}

func TestNewSearchCursorFromString(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		res, err := NewSearchCursorFromString("", "any")
		require.NoError(t, err)
		require.Nil(t, res)
	})
	t.Run("not a Base64", func(t *testing.T) {
		_, err := NewSearchCursorFromString("???", "any")
		require.ErrorContains(t, err, "decode cursor from Base64")
	})
	t.Run("no attribute", func(t *testing.T) {
		t.Run("undersize", func(t *testing.T) {
			_, err := NewSearchCursorFromString("q/WZCxCa19Y5lnEkCl/eL3TuEQdRmEtItzOe8TdsJA==", "")
			require.EqualError(t, err, "wrong OID cursor len 31")
		})
		t.Run("oversize", func(t *testing.T) {
			_, err := NewSearchCursorFromString("ebTksjW7LcatKlCnNIiqQXyhZKdD2iMvcDsYSokVYyYB", "")
			require.EqualError(t, err, "wrong OID cursor len 33")
		})
		id := oidtest.ID()
		res, err := NewSearchCursorFromString(base64.StdEncoding.EncodeToString(id[:]), "")
		require.NoError(t, err)
		require.NotEmpty(t, res)
		require.Equal(t, id[:], res.Key[1:])
		require.Zero(t, res.ValIDOff)
	})
	const attr = "any_attr"
	t.Run("header overflow", func(t *testing.T) {
		b := make([]byte, object.MaxHeaderLen+1)
		rand.Read(b) //nolint:staticcheck
		_, err := NewSearchCursorFromString(base64.StdEncoding.EncodeToString(b), attr)
		require.EqualError(t, err, "cursor len 16385 exceeds the limit 16384")
	})
	t.Run("no delimiter", func(t *testing.T) {
		_, err := NewSearchCursorFromString(base64.StdEncoding.EncodeToString([]byte(attr)), attr)
		require.EqualError(t, err, "missing delimiter")
	})
	t.Run("wrong attribute", func(t *testing.T) {
		b := slices.Concat([]byte(attr+"other"), utf8Delimiter)
		_, err := NewSearchCursorFromString(base64.StdEncoding.EncodeToString(b), attr)
		require.EqualError(t, err, "wrong attribute")
	})
	t.Run("no value", func(t *testing.T) {
		b := slices.Concat([]byte(attr), utf8Delimiter)
		_, err := NewSearchCursorFromString(base64.StdEncoding.EncodeToString(b), attr)
		require.EqualError(t, err, "missing value")
	})
	const val = "any_val"
	id := oidtest.ID()
	b := slices.Concat([]byte(attr), utf8Delimiter, []byte(val), id[:])
	res, err := NewSearchCursorFromString(base64.StdEncoding.EncodeToString(b), attr)
	require.NoError(t, err)
	require.NotEmpty(t, res.Key)
	require.Equal(t, b, res.Key[1:])
	require.Equal(t, 1+len(attr)+utf8DelimiterLen, res.ValIDOff)
}

func TestDB_SearchObjects(t *testing.T) {
	db := newDB(t)
	t.Run("no filters", func(t *testing.T) {
		t.Run("BoltDB failure", func(t *testing.T) {
			db := newDB(t)
			require.NoError(t, db.boltDB.Close())
			_, _, err := db.Search(cidtest.ID(), nil, nil, nil, nil, 1)
			require.ErrorContains(t, err, "view BoltDB")
			require.ErrorIs(t, err, bbolt.ErrDatabaseNotOpen)
		})

		cnr := cidtest.ID()

		t.Run("no objects", func(t *testing.T) {
			res, cursor, err := db.Search(cnr, nil, nil, nil, nil, 1)
			require.NoError(t, err)
			require.Empty(t, cursor)
			require.Empty(t, res)
		})

		const n = 10
		ids := oidtest.IDs(n)
		objs := make([]object.Object, n)
		for i := range objs {
			objs[i].SetContainerID(cnr)
			objs[i].SetID(ids[i])
			objs[i].SetPayloadChecksum(checksumtest.Checksum()) // required to Put

			err := db.Put(&objs[i], nil, nil)
			require.NoError(t, err, i)
		}

		idsSorted := sortObjectIDs(ids)

		t.Run("all at once", func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				count uint16
			}{
				{name: "exact", count: n},
				{name: "more", count: n + 1},
			} {
				res, cursor, err := db.Search(cnr, nil, nil, nil, nil, tc.count)
				require.NoError(t, err)
				require.Empty(t, cursor)
				require.Len(t, res, n)
				for i := range idsSorted {
					require.Equal(t, idsSorted[i], res[i].ID, i)
					require.Empty(t, res[i].Attributes)
				}
			}
		})
		t.Run("paginated", func(t *testing.T) {
			// request 3 first
			res, cursor, err := db.Search(cnr, nil, nil, nil, nil, 3)
			require.NoError(t, err)
			require.Len(t, res, 3)
			require.NotNil(t, cursor)
			for i := range 3 {
				require.Equal(t, idsSorted[i], res[i].ID, i)
				require.Empty(t, res[i].Attributes)
			}
			// then 6 more
			cursor.Key = slices.Concat([]byte{0}, cursor.Key)
			res, cursor, err = db.Search(cnr, nil, nil, nil, cursor, 6)
			require.NoError(t, err)
			require.Len(t, res, 6)
			require.NotNil(t, cursor)
			for i := range 6 {
				require.Equal(t, idsSorted[3+i], res[i].ID, i)
				require.Empty(t, res[i].Attributes)
			}
			// and up to 2 more
			cursor.Key = slices.Concat([]byte{0}, cursor.Key)
			res, cursor, err = db.Search(cnr, nil, nil, nil, cursor, 2)
			require.NoError(t, err)
			require.Nil(t, cursor)
			require.Len(t, res, 1)
			require.Equal(t, idsSorted[n-1], res[0].ID)
			require.Empty(t, res[0].Attributes)
		})
		t.Run("corrupted element", func(t *testing.T) {
			err := db.boltDB.Update(func(tx *bbolt.Tx) error {
				mbk := [1 + cid.Size]byte{0xFF}
				copy(mbk[1:], cnr[:])
				mb := tx.Bucket(mbk[:])
				require.NotNil(t, mb)

				k := [1 + oid.Size]byte{0x00}
				copy(k[1:], ids[rand.Intn(len(ids))][:])
				v := mb.Get(k[:])
				require.NotNil(t, v)

				return mb.Put(k[:len(k)-1], nil)
			})
			require.NoError(t, err)

			_, _, err = db.Search(cnr, nil, nil, nil, nil, n)
			require.EqualError(t, err, "view BoltDB: invalid meta bucket key (prefix 0x0): unexpected object key len 32")
		})
	})
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
			*object.NewAttribute("group_attr_1", "group_val_1"),
			*object.NewAttribute("group_attr_2", "group_val_2"),
		}
		types := [nRoot]object.Type{object.TypeRegular, object.TypeStorageGroup}
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
				*object.NewAttribute("attr_common", "val_common"),
				*object.NewAttribute("unique_attr_"+si, "unique_val_"+si),
				groupAttrs[nGroup],
				*object.NewAttribute("global_non_integer", "not an integer"),
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
			require.NoError(t, db.Put(&phys[i], nil, nil))
		}

		check := func(k string, m object.SearchMatchType, v string, matchInds []uint) {
			var fs object.SearchFilters
			fs.AddFilter(k, v, m)
			fInt, ok := PreprocessIntFilters(fs)
			if !ok {
				require.Empty(t, matchInds)
				return
			}
			require.True(t, ok)
			res, cursor, err := db.Search(cnr, fs, fInt, nil, nil, nAll)
			require.NoError(t, err)
			require.Empty(t, cursor)
			require.Len(t, res, len(matchInds))
			for i, ind := range matchInds {
				require.Equal(t, ids[ind], res[i].ID)
				require.Empty(t, res[i].Attributes)
			}
		}

		t.Run("all", func(t *testing.T) {
			check("attr_common", object.MatchStringEqual, "val_common", all)
		})
		t.Run("user attributes", func(t *testing.T) {
			// unique
			for i := range all {
				si := strconv.Itoa(i)
				key := "unique_attr_" + si
				val := "unique_val_" + si
				check(key, object.MatchStringEqual, val, []uint{uint(i)})
				check(key, object.MatchStringNotEqual, "other_val", []uint{uint(i)})
				for j := range val {
					check(key, object.MatchCommonPrefix, val[:j], []uint{uint(i)})
				}
				for _, matcher := range []object.SearchMatchType{
					object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
				} {
					check(key, matcher, val, nil)
				}
				var others []uint
				for j := range all {
					if j != i {
						others = append(others, uint(j))
					}
				}
				check(key, object.MatchNotPresent, "", others)
			}
			// group
			const val1 = "group_val_1"
			check("group_attr_1", object.MatchStringEqual, val1, group1)
			check("group_attr_1", object.MatchStringNotEqual, val1, nil)
			check("group_attr_1", object.MatchNotPresent, val1, group2)
			for i := range val1 {
				check("group_attr_1", object.MatchCommonPrefix, val1[:i], group1)
			}
			const val2 = "group_val_2"
			check("group_attr_2", object.MatchStringEqual, val2, group2)
			check("group_attr_2", object.MatchStringNotEqual, val2, nil)
			check("group_attr_2", object.MatchNotPresent, val2, group1)
			for i := range val1 {
				check("group_attr_2", object.MatchCommonPrefix, val2[:i], group2)
			}
			for _, matcher := range []object.SearchMatchType{
				object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check("group_attr_1", matcher, val1, nil)
				check("group_attr_2", matcher, val2, nil)
			}
		})
		t.Run("ROOT", func(t *testing.T) {
			for _, matcher := range []object.SearchMatchType{
				object.MatchUnspecified, object.MatchStringEqual, object.MatchStringNotEqual,
				object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check("$Object:ROOT", matcher, "", []uint{0})
			}
			check("$Object:ROOT", object.MatchNotPresent, "", nil)
		})
		t.Run("PHY", func(t *testing.T) {
			for _, matcher := range []object.SearchMatchType{
				object.MatchUnspecified, object.MatchStringEqual, object.MatchStringNotEqual,
				object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE,
			} {
				check("$Object:PHY", matcher, "", []uint{2, 3, 4, 5})
			}
			check("$Object:PHY", object.MatchNotPresent, "", nil)
		})
		t.Run("version", func(t *testing.T) {
			check := func(m object.SearchMatchType, v string, matchInds []uint) {
				check("$Object:version", m, v, matchInds)
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
				check("$Object:ownerID", m, v, matchInds)
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
				check("$Object:objectType", m, v, matchInds)
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
				check("$Object:payloadHash", m, v, matchInds)
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
				check("$Object:homomorphicHash", m, v, matchInds)
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
		t.Run("split ID", func(t *testing.T) {
			check := func(m object.SearchMatchType, v string, matchInds []uint) {
				check("$Object:split.splitID", m, v, matchInds)
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
		t.Run("first ID", func(t *testing.T) {
			check := func(m object.SearchMatchType, v string, matchInds []uint) {
				check("$Object:split.first", m, v, matchInds)
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
				check("$Object:split.parent", m, v, matchInds)
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
				check("global_non_integer", matcher, "123", nil)
				// TODO: also check that BoltDB is untouched in following cases
				check("attr_int", matcher, "text", nil)
				check("attr_int", matcher, "1.5", nil)
			}
			check("attr_int", object.MatchNumLT, "-115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check("attr_int", object.MatchNumLE, "-115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check("attr_int", object.MatchNumGT, "-115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check("attr_int", object.MatchNumGE, "-115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check("attr_int", object.MatchNumLT, "115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check("attr_int", object.MatchNumLE, "115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check("attr_int", object.MatchNumGT, "115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check("attr_int", object.MatchNumGE, "115792089237316195423570985008687907853269984665640564039457584007913129639936", nil)
			check("attr_int", object.MatchNumLT, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", nil)
			check("attr_int", object.MatchNumLT, "-18446744073709551615", []uint{0})
			check("attr_int", object.MatchNumLT, "0", []uint{0, 2})
			check("attr_int", object.MatchNumLT, "18446744073709551615", []uint{0, 2, 3})
			check("attr_int", object.MatchNumLT, "115792089237316195423570985008687907853269984665640564039457584007913129639935", []uint{0, 2, 3, 4})
			check("attr_int", object.MatchNumLE, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", []uint{0})
			check("attr_int", object.MatchNumLE, "-18446744073709551615", []uint{0, 2})
			check("attr_int", object.MatchNumLE, "0", []uint{0, 2, 3})
			check("attr_int", object.MatchNumLE, "18446744073709551615", []uint{0, 2, 3, 4})
			check("attr_int", object.MatchNumLE, "115792089237316195423570985008687907853269984665640564039457584007913129639935", allInt)
			check("attr_int", object.MatchNumGT, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", []uint{1, 2, 3, 4})
			check("attr_int", object.MatchNumGT, "-18446744073709551615", []uint{1, 3, 4})
			check("attr_int", object.MatchNumGT, "0", []uint{1, 4})
			check("attr_int", object.MatchNumGT, "18446744073709551615", []uint{1})
			check("attr_int", object.MatchNumGT, "115792089237316195423570985008687907853269984665640564039457584007913129639935", nil)
			check("attr_int", object.MatchNumGE, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", allInt)
			check("attr_int", object.MatchNumGE, "-18446744073709551615", []uint{1, 2, 3, 4})
			check("attr_int", object.MatchNumGE, "0", []uint{1, 3, 4})
			check("attr_int", object.MatchNumGE, "18446744073709551615", []uint{1, 4})
			check("attr_int", object.MatchNumGE, "115792089237316195423570985008687907853269984665640564039457584007913129639935", []uint{1})
			for _, tc := range []struct {
				name, key  string
				val1, val2 string
			}{
				{name: "creation epoch", key: "$Object:creationEpoch", val1: "10", val2: "11"},
				{name: "payload length", key: "$Object:payloadLength", val1: "20", val2: "21"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					check(tc.key, object.MatchNumLT, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", nil)
					check(tc.key, object.MatchNumLT, "0", nil)
					check(tc.key, object.MatchNumLT, tc.val1, nil)
					check(tc.key, object.MatchNumLT, tc.val2, group1)
					check(tc.key, object.MatchNumLT, "18446744073709551615", all)
					check(tc.key, object.MatchNumLT, "115792089237316195423570985008687907853269984665640564039457584007913129639935", all)
					check(tc.key, object.MatchNumLE, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", nil)
					check(tc.key, object.MatchNumLE, "0", nil)
					check(tc.key, object.MatchNumLE, tc.val1, group1)
					check(tc.key, object.MatchNumLE, tc.val2, all)
					check(tc.key, object.MatchNumLE, "18446744073709551615", all)
					check(tc.key, object.MatchNumLE, "115792089237316195423570985008687907853269984665640564039457584007913129639935", all)
					check(tc.key, object.MatchNumGT, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", all)
					check(tc.key, object.MatchNumGT, "0", all)
					check(tc.key, object.MatchNumGT, tc.val1, group2)
					check(tc.key, object.MatchNumGT, tc.val2, nil)
					check(tc.key, object.MatchNumGT, "18446744073709551615", nil)
					check(tc.key, object.MatchNumGT, "115792089237316195423570985008687907853269984665640564039457584007913129639935", nil)
					check(tc.key, object.MatchNumGE, "-115792089237316195423570985008687907853269984665640564039457584007913129639935", all)
					check(tc.key, object.MatchNumGE, "0", all)
					check(tc.key, object.MatchNumGE, tc.val1, all)
					check(tc.key, object.MatchNumGE, tc.val2, group2)
					check(tc.key, object.MatchNumGE, "18446744073709551615", nil)
					check(tc.key, object.MatchNumGE, "115792089237316195423570985008687907853269984665640564039457584007913129639935", nil)
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
					objs[i].SetPayloadChecksum(checksumtest.Checksum()) // Put requires
					require.NoError(t, db.Put(&objs[i], nil, nil))
				}
				check := func(tb testing.TB, m object.SearchMatchType, val string, inds ...int) {
					var fs object.SearchFilters
					fs.AddFilter(attr, val, m)
					fInt, ok := PreprocessIntFilters(fs)
					require.True(t, ok)
					res, _, err := db.Search(cnr, fs, fInt, []string{attr}, nil, 1000)
					require.NoError(t, err)
					require.Len(t, res, len(inds))
					for i, ind := range inds {
						require.Equal(t, ids[ind], res[i].ID, vals[i])
					}
				}
				all := make([]int, len(vals))
				for i := range vals {
					all[i] = i
				}
				t.Run("EQ", func(t *testing.T) {
					for i := range vals {
						check(t, object.MatchStringEqual, vals[i], i)
					}
				})
				t.Run("NE", func(t *testing.T) {
					for i := range vals {
						others := make([]int, 0, len(vals)-1)
						for j := range vals {
							if j != i {
								others = append(others, j)
							}
						}
						check(t, object.MatchStringNotEqual, vals[i], others...)
					}
					t.Run("jump while not yet limit", func(t *testing.T) {
						// iterator starts from int attribute key space, and once it is finished - jumps
						// to the plain one. It must not forget to check plain attributes since there
						// can be matching elements that should be returned according to the sorting.
						var fs object.SearchFilters
						fs.AddFilter(attr, "missing", object.MatchStringNotEqual)
						fInt, ok := PreprocessIntFilters(fs)
						require.True(t, ok)
						res, cursor, err := db.Search(cnr, fs, fInt, []string{attr}, nil, 2)
						require.NoError(t, err)
						require.Len(t, res, 2)
						require.Equal(t, ids[0], res[0].ID)
						require.Equal(t, ids[1], res[1].ID)
						require.NotEmpty(t, cursor)
					})
					t.Run("empty", func(t *testing.T) {
						check(t, object.MatchStringNotEqual, "", all...)
					})
				})
				t.Run("PREFIX", func(t *testing.T) {
					t.Run("negative", func(t *testing.T) {
						check := func(t testing.TB, val string) {
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
			type filter struct {
				k string
				m object.SearchMatchType
				v string
			}
			for _, tc := range []struct {
				is []uint
				fs []filter
			}{
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
				{is: []uint{2, 4}, fs: []filter{
					{k: "$Object:split.splitID", m: object.MatchCommonPrefix, v: "8b69e76d-5e95-4639-8213-46786c41ab73"},
					{k: "random", m: object.MatchNotPresent},
					{k: "attr_common", m: object.MatchStringNotEqual, v: "random"},
				}},
			} {
				t.Run("complex", func(t *testing.T) {
					var fs object.SearchFilters
					for _, f := range tc.fs {
						fs.AddFilter(f.k, f.v, f.m)
					}
					fInt, ok := PreprocessIntFilters(fs)
					if !ok {
						require.Empty(t, tc.is)
						return
					}
					res, cursor, err := db.Search(cnr, fs, fInt, nil, nil, nAll)
					require.NoError(t, err)
					require.Empty(t, cursor)
					require.Len(t, res, len(tc.is))
					for i, ind := range tc.is {
						require.Equal(t, ids[ind], res[i].ID)
						require.Empty(t, res[i].Attributes)
					}
				})
			}
		})
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
				objs[i].SetPayloadChecksum(checksumtest.Checksum()) // Put requires
				require.NoError(t, db.Put(&objs[i], nil, nil))
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
					fInt, ok := PreprocessIntFilters(fs)
					require.True(t, ok)
					res, cursor, err := db.Search(cnr, fs, fInt, nil, nil, 1000)
					require.NoError(t, err)
					require.Empty(t, cursor)
					require.Empty(t, res)
				}
			})
			t.Run("all", func(t *testing.T) {
				t.Run("unfiltered", func(t *testing.T) {
					res, cursor, err := db.Search(cnr, nil, nil, nil, nil, 1000)
					require.NoError(t, err)
					require.Empty(t, cursor)
					require.Len(t, res, len(objs))
					for i := range res {
						require.Equal(t, ids[i], res[i].ID)
						require.Empty(t, res[i].Attributes)
					}
				})
				var fs object.SearchFilters
				fs.AddFilter(heightAttr, "0", object.MatchNumGE)
				t.Run("w/o attributes", func(t *testing.T) {
					res, cursor, err := db.Search(cnr, fs, nil, nil, nil, 1000)
					require.NoError(t, err)
					require.Empty(t, cursor)
					require.Len(t, res, len(heightSorted))
					for i := range res {
						// expected order by IDs
						require.Equal(t, ids[i], res[i].ID)
						require.Empty(t, res[i].Attributes)
					}
					t.Run("paging", func(t *testing.T) {
						res, cursor, err := db.Search(cnr, fs, nil, nil, nil, 2)
						require.NoError(t, err)
						require.Len(t, res, 2)
						for i := range 2 {
							require.Equal(t, ids[i], res[i].ID)
							require.Empty(t, res[i].Attributes)
						}
						require.NotEmpty(t, cursor)
						cursor.Key = slices.Concat([]byte{0}, cursor.Key)
						//nolint:staticcheck // drop with t.Skip
						res, cursor, err = db.Search(cnr, fs, nil, nil, cursor, 6)
						require.NoError(t, err)
						t.Skip("paging is broken when prim attribute is not requested, see also https://github.com/nspcc-dev/neofs-node/issues/3058#issuecomment-2553193094")
						require.Len(t, res, 6)
						for i := range 6 {
							require.Equal(t, ids[2+i], res[i].ID, i)
							require.Empty(t, res[i].Attributes)
						}
						require.NotEmpty(t, cursor)
						cursor.Key = slices.Concat([]byte{0}, cursor.Key)
						res, cursor, err = db.Search(cnr, fs, nil, nil, cursor, 3)
						require.NoError(t, err)
						require.Len(t, res, 2)
						for i := range 2 {
							require.Equal(t, ids[8+i], res[i].ID)
							require.Empty(t, res[i].Attributes)
						}
						require.Empty(t, cursor)
					})
				})
				t.Run("single attribute", func(t *testing.T) {
					res, cursor, err := db.Search(cnr, fs, nil, []string{heightAttr}, nil, 1000)
					require.NoError(t, err)
					require.Empty(t, cursor)
					require.Len(t, res, len(heightSorted))
					for i, r := range heightSorted {
						require.Equal(t, r.ID, res[i].ID)
						require.Equal(t, []string{r.Attributes[0]}, res[i].Attributes)
					}
				})
				t.Run("two attributes", func(t *testing.T) {
					res, cursor, err := db.Search(cnr, fs, nil, []string{heightAttr, otherAttr}, nil, 1000)
					require.NoError(t, err)
					require.Empty(t, cursor)
					require.Equal(t, heightSorted, res)
					t.Run("paging", func(t *testing.T) {
						res, cursor, err := db.Search(cnr, fs, nil, []string{heightAttr, otherAttr}, nil, 2)
						require.NoError(t, err)
						require.Equal(t, heightSorted[:2], res)
						require.NotEmpty(t, cursor)
						cursor.Key = slices.Concat([]byte{0}, cursor.Key)
						res, cursor, err = db.Search(cnr, fs, nil, []string{heightAttr, otherAttr}, cursor, 6)
						require.NoError(t, err)
						require.Equal(t, heightSorted[2:8], res)
						require.NotEmpty(t, cursor)
						cursor.Key = slices.Concat([]byte{0}, cursor.Key)
						res, cursor, err = db.Search(cnr, fs, nil, []string{heightAttr, otherAttr}, cursor, 3)
						require.NoError(t, err)
						require.Equal(t, heightSorted[8:], res)
						require.Empty(t, cursor)
					})
				})
			})
			t.Run("partial", func(t *testing.T) {
				var fs object.SearchFilters
				fs.AddFilter(heightAttr, "50", object.MatchNumGE)
				fs.AddFilter(heightAttr, "150", object.MatchNumLE)
				heightSorted := heightSorted[2:9]
				ids := ids[1:8]
				t.Run("w/o attributes", func(t *testing.T) {
					fInt, ok := PreprocessIntFilters(fs)
					require.True(t, ok)
					res, cursor, err := db.Search(cnr, fs, fInt, nil, nil, 1000)
					require.NoError(t, err)
					require.Empty(t, cursor)
					require.Len(t, res, len(ids))
					for i := range res {
						// expected order by IDs
						require.Equal(t, ids[i], res[i].ID)
						require.Empty(t, res[i].Attributes)
					}
					t.Run("paging", func(t *testing.T) {
						fInt, ok := PreprocessIntFilters(fs)
						require.True(t, ok)
						res, cursor, err := db.Search(cnr, fs, fInt, nil, nil, 2)
						require.NoError(t, err)
						require.Len(t, res, 2)
						for i := range 2 {
							require.Equal(t, ids[i], res[i].ID)
							require.Empty(t, res[i].Attributes)
						}
						require.NotEmpty(t, cursor)
						cursor.Key = slices.Concat([]byte{0}, cursor.Key)
						//nolint:staticcheck // drop with t.Skip
						res, cursor, err = db.Search(cnr, fs, nil, nil, cursor, 6)
						require.NoError(t, err)
						t.Skip("paging is broken when prim attribute is not requested, see also https://github.com/nspcc-dev/neofs-node/issues/3058#issuecomment-2553193094")
						require.Len(t, res, 6)
						for i := range 6 {
							require.Equal(t, ids[2+i], res[i].ID, i)
							require.Empty(t, res[i].Attributes)
						}
						require.NotEmpty(t, cursor)
						res, cursor, err = db.Search(cnr, fs, nil, nil, cursor, 3)
						require.NoError(t, err)
						require.Len(t, res, 2)
						for i := range 2 {
							require.Equal(t, ids[8+i], res[i].ID)
							require.Empty(t, res[i].Attributes)
						}
						require.Empty(t, cursor)
					})
				})
				t.Run("single attribute", func(t *testing.T) {
					fInt, ok := PreprocessIntFilters(fs)
					require.True(t, ok)
					res, cursor, err := db.Search(cnr, fs, fInt, []string{heightAttr}, nil, 1000)
					require.NoError(t, err)
					require.Empty(t, cursor)
					require.Len(t, res, len(heightSorted))
					for i, r := range heightSorted {
						require.Equal(t, r.ID, res[i].ID)
						require.Equal(t, []string{r.Attributes[0]}, res[i].Attributes)
					}
				})
				t.Run("two attributes", func(t *testing.T) {
					fInt, ok := PreprocessIntFilters(fs)
					require.True(t, ok)
					res, cursor, err := db.Search(cnr, fs, fInt, []string{heightAttr, otherAttr}, nil, 1000)
					require.NoError(t, err)
					require.Empty(t, cursor)
					require.Equal(t, heightSorted, res)
					t.Run("paging", func(t *testing.T) {
						fInt, ok := PreprocessIntFilters(fs)
						require.True(t, ok)
						res, cursor, err := db.Search(cnr, fs, fInt, []string{heightAttr, otherAttr}, nil, 2)
						require.NoError(t, err)
						require.Equal(t, heightSorted[:2], res)
						require.NotEmpty(t, cursor)
						cursor.Key = slices.Concat([]byte{0}, cursor.Key)
						res, cursor, err = db.Search(cnr, fs, fInt, []string{heightAttr, otherAttr}, cursor, 3)
						require.NoError(t, err)
						require.Equal(t, heightSorted[2:5], res)
						require.NotEmpty(t, cursor)
						cursor.Key = slices.Concat([]byte{0}, cursor.Key)
						res, cursor, err = db.Search(cnr, fs, fInt, []string{heightAttr, otherAttr}, cursor, 3)
						require.NoError(t, err)
						require.Equal(t, heightSorted[5:], res)
						require.Empty(t, cursor)
					})
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
				objs[i].SetPayloadChecksum(checksumtest.Checksum()) // Put requires
				require.NoError(t, db.Put(&objs[i], nil, nil))
			}
			t.Run("none", func(t *testing.T) {
				var fs object.SearchFilters
				fs.AddFilter(object.AttributeFilePath, "cat4.jpg", object.MatchStringEqual)
				res, cursor, err := db.Search(cnr, fs, nil, []string{object.AttributeFilePath, object.AttributeTimestamp}, nil, 1000)
				require.NoError(t, err)
				require.Empty(t, cursor)
				require.Empty(t, res)
			})
			t.Run("single", func(t *testing.T) {
				var fs object.SearchFilters
				fs.AddFilter(object.AttributeFilePath, "cat1.jpg", object.MatchStringEqual)
				res, cursor, err := db.Search(cnr, fs, nil, []string{object.AttributeFilePath, object.AttributeTimestamp}, nil, 1000)
				require.NoError(t, err)
				require.Empty(t, cursor)
				require.Equal(t, []client.SearchResultItem{
					{ID: ids[0], Attributes: []string{"cat1.jpg", "1738760790"}},
				}, res)
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
					res, cursor, err := db.Search(cnr, fs, nil, []string{object.AttributeFilePath, object.AttributeTimestamp}, nil, 1000)
					require.NoError(t, err)
					require.Equal(t, fullRes, res)
					require.Empty(t, cursor)
					t.Run("paging", func(t *testing.T) {
						res, cursor, err := db.Search(cnr, fs, nil, []string{object.AttributeFilePath, object.AttributeTimestamp}, nil, 2)
						require.NoError(t, err)
						require.Equal(t, fullRes[:2], res)
						require.NotEmpty(t, cursor)
						cursor.Key = slices.Concat([]byte{0}, cursor.Key)
						res, cursor, err = db.Search(cnr, fs, nil, []string{object.AttributeFilePath, object.AttributeTimestamp}, cursor, 1000)
						require.NoError(t, err)
						require.Equal(t, fullRes[2:], res)
						require.Empty(t, cursor)
					})
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
				objs[i].SetPayloadChecksum(checksumtest.Checksum()) // Put requires
				require.NoError(t, db.Put(&objs[i], nil, nil))
			}

			attrs := []string{object.AttributeFilePath, "attr1", "attr2"}

			var fs object.SearchFilters
			fs.AddFilter(object.AttributeFilePath, "/home/Downloads/", object.MatchCommonPrefix)
			fs.AddFilter("Type", "JPEG", object.MatchStringEqual)
			res, cursor, err := db.Search(cnr, fs, nil, attrs, nil, 1000)
			require.NoError(t, err)
			require.Empty(t, cursor)
			require.Equal(t, []client.SearchResultItem{
				{ID: ids[1], Attributes: []string{"/home/Downloads/cat.jpg", "val1_3", "val2_3"}},
				{ID: ids[3], Attributes: []string{"/home/Downloads/dog.jpg", "val1_1", "val2_1"}},
			}, res)

			fs = fs[:0]
			fs.AddFilter(object.AttributeFilePath, "/usr", object.MatchCommonPrefix)
			fs.AddFilter("Type", "BIN", object.MatchStringEqual)
			res, cursor, err = db.Search(cnr, fs, nil, attrs, nil, 1000)
			require.NoError(t, err)
			require.Empty(t, cursor)
			require.Equal(t, []client.SearchResultItem{
				{ID: ids[2], Attributes: []string{"/usr/local/bin/go", "val1_2", "val2_2"}},
			}, res)

			fs = fs[:0]
			fs.AddFilter(object.AttributeFilePath, "/", object.MatchCommonPrefix)
			fs.AddFilter("Type", "BIN", object.MatchStringNotEqual)
			res, cursor, err = db.Search(cnr, fs, nil, attrs, nil, 1000)
			require.NoError(t, err)
			require.Empty(t, cursor)
			require.Equal(t, []client.SearchResultItem{
				{ID: ids[1], Attributes: []string{"/home/Downloads/cat.jpg", "val1_3", "val2_3"}},
				{ID: ids[3], Attributes: []string{"/home/Downloads/dog.jpg", "val1_1", "val2_1"}},
				{ID: ids[0], Attributes: []string{"/var/log/neofs/node", "val1_4", "val2_4"}},
			}, res)

			t.Run("paging", func(t *testing.T) {
				fs = fs[:0]
				fs.AddFilter(object.AttributeFilePath, "/home/", object.MatchCommonPrefix)
				fs.AddFilter("Type", "TEXT", object.MatchStringNotEqual)
				res, cursor, err := db.Search(cnr, fs, nil, attrs, nil, 1)
				require.NoError(t, err)
				require.Equal(t, []client.SearchResultItem{
					{ID: ids[1], Attributes: []string{"/home/Downloads/cat.jpg", "val1_3", "val2_3"}},
				}, res)
				require.NotEmpty(t, cursor)
				cursor.Key = slices.Concat([]byte{0}, cursor.Key)
				res, cursor, err = db.Search(cnr, fs, nil, attrs, cursor, 1)
				require.NoError(t, err)
				require.Equal(t, []client.SearchResultItem{
					{ID: ids[3], Attributes: []string{"/home/Downloads/dog.jpg", "val1_1", "val2_1"}},
				}, res)
				require.Empty(t, cursor)
			})
		})
	})
	t.Run("GC", func(t *testing.T) {
		s := testEpochState(10)
		db := newDB(t, WithEpochState(s))
		ids := sortObjectIDs(oidtest.IDs(5))
		objs := make([]object.Object, len(ids))
		const attrPlain, valPlain = "Plain", "Value"
		const attrInt, valInt = "Int", "100"
		// store
		cnr := cidtest.ID()
		for i := range objs {
			appendAttribute(&objs[i], attrPlain, valPlain)
			appendAttribute(&objs[i], attrInt, valInt)
			appendAttribute(&objs[i], "IntOverflow", "115792089237316195423570985008687907853269984665640564039457584007913129639936")
			appendAttribute(&objs[i], "IntOverflowNeg", "-115792089237316195423570985008687907853269984665640564039457584007913129639936")
			objs[i].SetID(ids[i])
			objs[i].SetContainerID(cnr)
			if i == 3 {
				appendAttribute(&objs[i], object.AttributeExpirationEpoch, "11")
			}
			objs[i].SetPayloadChecksum(checksumtest.Checksum()) // Put requires
			require.NoError(t, db.Put(&objs[i], nil, nil))
		}

		check := func(t testing.TB, exp []oid.ID) {
			check := func(res []client.SearchResultItem, err error) {
				require.NoError(t, err)
				require.Len(t, res, len(exp))
				for i := range exp {
					require.Equal(t, exp[i], res[i].ID)
				}
			}
			res, _, err := db.Search(cnr, nil, nil, nil, nil, 1000)
			check(res, err)
			var fs object.SearchFilters
			fs.AddFilter(attrPlain, valPlain, object.MatchStringEqual)
			res, _, err = db.Search(cnr, fs, nil, []string{attrPlain}, nil, 1000)
			check(res, err)
			fs = fs[:0]
			fs.AddFilter(attrInt, valInt, object.MatchStringEqual)
			res, _, err = db.Search(cnr, fs, nil, []string{attrInt}, nil, 1000)
			check(res, err)
		}
		// all available
		check(t, ids)
		t.Run("garbage mark", func(t *testing.T) {
			_, _, err := db.MarkGarbage(false, false, oid.NewAddress(cnr, ids[1]))
			require.NoError(t, err)
			check(t, slices.Concat(ids[:1], ids[2:]))
			// lock resurrects the object
			err = db.Lock(cnr, oidtest.ID(), []oid.ID{ids[1]})
			require.NoError(t, err)
			check(t, ids)
		})
		t.Run("tombstone", func(t *testing.T) {
			_, _, err := db.Inhume(oid.NewAddress(cnr, oidtest.ID()), math.MaxUint64, false, oid.NewAddress(cnr, ids[2]))
			require.NoError(t, err)
			check(t, slices.Concat(ids[:2], ids[3:]))
			// lock resurrects the object
			err = db.Lock(cnr, oidtest.ID(), []oid.ID{ids[2]})
			require.NoError(t, err)
			check(t, ids)
		})
		t.Run("expired", func(t *testing.T) {
			*s++
			check(t, ids)
			*s++
			check(t, slices.Concat(ids[:3], ids[4:]))
			check(t, slices.Concat(ids[:3], ids[4:]))
		})
		t.Run("rm", func(t *testing.T) {
			_, err := db.Delete([]oid.Address{oid.NewAddress(cnr, ids[4])})
			require.NoError(t, err)
			check(t, slices.Concat(ids[:3], ids[5:]))
		})
	})
}
