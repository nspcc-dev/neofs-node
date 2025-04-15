package meta

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"maps"
	"math"
	"math/big"
	"math/rand"
	"slices"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	metatest "github.com/nspcc-dev/neofs-node/pkg/util/meta/test"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	checksumtest "github.com/nspcc-dev/neofs-sdk-go/checksum/test"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func sortObjectIDs(ids []oid.ID) []oid.ID {
	s := slices.Clone(ids)
	slices.SortFunc(s, func(a, b oid.ID) int { return bytes.Compare(a[:], b[:]) })
	return s
}

func searchResultForIDs(ids []oid.ID) []client.SearchResultItem {
	res := make([]client.SearchResultItem, len(ids))
	for i := range ids {
		res[i].ID = ids[i]
	}
	return res
}

func appendAttribute(obj *object.Object, k, v string) {
	obj.SetAttributes(append(obj.Attributes(), object.NewAttribute(k, v))...)
}

func assertPrefixedAttrIDPresence[T string | []byte](t testing.TB, mb *bbolt.Bucket, id oid.ID, isInt bool, attr string, val T, exp bool) {
	var prefix byte
	if isInt {
		prefix = 0x01
	} else {
		prefix = 0x02
	}
	k := []byte{prefix}
	k = append(k, attr...)
	k = append(k, 0x00)
	k = append(k, val...)
	if !isInt {
		k = append(k, 0x00)
	}
	k = append(k, id[:]...)
	require.Equal(t, exp, mb.Get(k) != nil)
}

func assertAttrPresence[T string | []byte](t testing.TB, mb *bbolt.Bucket, id oid.ID, attr string, val T, exp bool) {
	assertPrefixedAttrIDPresence(t, mb, id, false, attr, val, exp)
	k := []byte{0x03}
	k = append(k, id[:]...)
	k = append(k, attr...)
	k = append(k, 0x00)
	k = append(k, val...)
	require.Equal(t, exp, mb.Get(k) != nil)
}

func assertAttr[T string | []byte](t testing.TB, mb *bbolt.Bucket, id oid.ID, attr string, val T) {
	assertAttrPresence(t, mb, id, attr, val, true)
}

func assertIntAttr(t testing.TB, mb *bbolt.Bucket, id oid.ID, attr string, origin string, val []byte) {
	assertAttr(t, mb, id, attr, origin)
	assertPrefixedAttrIDPresence(t, mb, id, true, attr, val, true)
}

func TestHeaderLimit(t *testing.T) { require.Less(t, object.MaxHeaderLen, bbolt.MaxKeySize) }

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
	addAttr := func(k, v string) { attrs = append(attrs, object.NewAttribute(k, v)) }
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

	t.Run("failure", func(t *testing.T) {
		t.Run("zero by in attribute", func(t *testing.T) {
			testWithAttr := func(t *testing.T, k, v, msg string) {
				obj := obj
				obj.SetAttributes(
					object.NewAttribute("valid key", "valid value"),
					object.NewAttribute(k, v),
				)
				require.EqualError(t, db.Put(&obj, nil, nil), msg)
			}
			t.Run("in key", func(t *testing.T) {
				testWithAttr(t, "k\x00y", "value", "attribute #1 key contains 0x00 byte used in sep")
			})
			t.Run("in value", func(t *testing.T) {
				testWithAttr(t, "key", "va\x00ue", "attribute #1 value contains 0x00 byte used in sep")
			})
		})
	})

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

	t.Run("no homomorphic checksum", func(t *testing.T) {
		cnr := cidtest.OtherID(cnr)
		var obj object.Object
		obj.SetContainerID(cnr)
		obj.SetID(id)
		obj.SetOwner(usertest.ID())     // Put requires
		obj.SetPayloadChecksum(pldHash) // Put requires

		require.NoError(t, db.Put(&obj, nil, nil))

		require.NoError(t, db.boltDB.View(func(tx *bbolt.Tx) error {
			mb := tx.Bucket(append([]byte{0xFF}, cnr[:]...))
			require.NotNil(t, mb, "missing container's meta bucket")
			assertAttrPresence(t, mb, id, "$Object:homomorphicHash", "", false)
			return nil
		}))
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
			if err := b.Put(objectcore.BigIntBytes(n), nil); err != nil {
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
			val, err := objectcore.RestoreIntAttribute(k)
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

func cloneIntFilterMap(src map[int]objectcore.ParsedIntFilter) map[int]objectcore.ParsedIntFilter {
	if src == nil {
		return nil
	}
	dst := maps.Clone(src)
	for k, f := range src {
		var n *big.Int
		if f.Parsed != nil {
			n = new(big.Int).Set(f.Parsed)
		}
		dst[k] = objectcore.ParsedIntFilter{
			AutoMatch: f.AutoMatch,
			Parsed:    n,
			Raw:       slices.Clone(f.Raw),
		}
	}
	return dst
}

func cloneSearchCursor(c *objectcore.SearchCursor) *objectcore.SearchCursor {
	if c == nil {
		return nil
	}
	return &objectcore.SearchCursor{PrimaryKeysPrefix: slices.Clone(c.PrimaryKeysPrefix), PrimarySeekKey: slices.Clone(c.PrimarySeekKey)}
}

func _assertSearchResultWithLimit(t testing.TB, db *DB, cnr cid.ID, fs object.SearchFilters, attrs []string, all []client.SearchResultItem, lim uint16) {
	var strCursor string
	nAttr := len(attrs)
	for {
		cursor, fInt, err := objectcore.PreprocessSearchQuery(fs, attrs, strCursor)
		if err != nil {
			if len(all) == 0 {
				require.ErrorIs(t, err, objectcore.ErrUnreachableQuery)
			} else {
				require.NoError(t, err)
			}
			return
		}

		cursorClone := cloneSearchCursor(cursor)
		fIntClone := cloneIntFilterMap(fInt)

		res, c, err := db.Search(cnr, fs, fInt, attrs, cursor, lim)
		require.Equal(t, cursorClone, cursor, "cursor mutation detected", "cursor: %q", strCursor)
		require.Equal(t, fIntClone, fInt, "int filter map mutation detected", "cursor: %q", strCursor)
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

		cc, err := objectcore.CalculateCursor(fs, res[n-1])
		require.NoError(t, err, "cursor: %q", strCursor)
		require.Equal(t, c, cc, "cursor: %q", strCursor)

		strCursor = base64.StdEncoding.EncodeToString(c)
	}
}

func assertSearchResultWithLimit(t testing.TB, db *DB, cnr cid.ID, fs object.SearchFilters, attrs []string, expRes []client.SearchResultItem, lim uint16) {
	_assertSearchResultWithLimit(t, db, cnr, fs, attrs, expRes, lim)
	if len(attrs) > 0 {
		expRes = slices.Clone(expRes)
		slices.SortFunc(expRes, func(a, b client.SearchResultItem) int { return bytes.Compare(a.ID[:], b.ID[:]) })
		_assertSearchResultWithLimit(t, db, cnr, fs, nil, expRes, lim)
	}
}

func assertSearchResult(t *testing.T, db *DB, cnr cid.ID, fs object.SearchFilters, attrs []string, expRes []client.SearchResultItem) {
	for _, lim := range []uint16{1000, 1, 2} {
		t.Run(fmt.Sprintf("limit=%d", lim), func(t *testing.T) {
			assertSearchResultWithLimit(t, db, cnr, fs, attrs, expRes, lim)
		})
	}
}

type searchTestDB struct {
	db *DB
}

func (s *searchTestDB) EmptyDB(t *testing.T) {
	require.NoError(t, s.db.Close())

	s.db = newDB(t)
}

func (s *searchTestDB) Put(obj *object.Object) error {
	return s.db.Put(obj, nil, nil)
}

func (s *searchTestDB) Search(cnr cid.ID, fs object.SearchFilters, fInt map[int]objectcore.ParsedIntFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	return s.db.Search(cnr, fs, fInt, attrs, cursor, count)
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
			assertSearchResult(t, db, cnr, nil, nil, nil)
		})

		const n = 10
		ids := oidtest.IDs(n)
		objs := make([]object.Object, n)
		for i := range objs {
			objs[i].SetContainerID(cnr)
			objs[i].SetID(ids[i])
			objs[i].SetOwner(usertest.ID())                     // required to Put
			objs[i].SetPayloadChecksum(checksumtest.Checksum()) // required to Put

			err := db.Put(&objs[i], nil, nil)
			require.NoError(t, err, i)
		}

		idsSorted := sortObjectIDs(ids)
		all := searchResultForIDs(idsSorted)

		t.Run("paginated", func(t *testing.T) {
			assertSearchResult(t, db, cnr, nil, nil, all)
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

			cursor, fInt, err := objectcore.PreprocessSearchQuery(nil, nil, "")
			require.NoError(t, err)

			_, _, err = db.Search(cnr, nil, fInt, nil, cursor, n)
			require.EqualError(t, err, "view BoltDB: invalid meta bucket key (prefix 0x0): unexpected object key len 32")
		})
	})
	t.Run("filtered results", func(t *testing.T) {
		metatest.TestSearchObjects(t, &searchTestDB{db: db}, true)
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
			objs[i].SetOwner(usertest.ID())                     // Put requires
			objs[i].SetPayloadChecksum(checksumtest.Checksum()) // Put requires
			require.NoError(t, db.Put(&objs[i], nil, nil))
		}

		check := func(t *testing.T, exp []oid.ID) {
			var fs object.SearchFilters
			fs.AddFilter(attrPlain, valPlain, object.MatchStringEqual)
			assertSearchResult(t, db, cnr, fs, nil, searchResultForIDs(exp))
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
