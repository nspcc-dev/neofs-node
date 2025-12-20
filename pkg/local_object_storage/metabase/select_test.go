package meta_test

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"math/big"
	"strconv"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
)

func TestDB_SelectUserAttributes(t *testing.T) {
	db := newDB(t)

	cnr := cidtest.ID()

	raw1 := generateObjectWithCID(t, cnr)
	addAttribute(raw1, "foo", "bar")
	addAttribute(raw1, "x", "y")

	err := putBig(db, raw1)
	require.NoError(t, err)

	raw2 := generateObjectWithCID(t, cnr)
	addAttribute(raw2, "foo", "bar")
	addAttribute(raw2, "x", "z")

	err = putBig(db, raw2)
	require.NoError(t, err)

	raw3 := generateObjectWithCID(t, cnr)
	addAttribute(raw3, "a", "b")

	err = putBig(db, raw3)
	require.NoError(t, err)

	raw4 := generateObjectWithCID(t, cnr)
	addAttribute(raw4, "path", "test/1/2")

	err = putBig(db, raw4)
	require.NoError(t, err)

	raw5 := generateObjectWithCID(t, cnr)
	addAttribute(raw5, "path", "test/1/3")

	err = putBig(db, raw5)
	require.NoError(t, err)

	raw6 := generateObjectWithCID(t, cnr)
	addAttribute(raw6, "path", "test/2/3")

	err = putBig(db, raw6)
	require.NoError(t, err)

	fs := object.SearchFilters{}
	fs.AddFilter("foo", "bar", object.MatchStringEqual)
	testSelect(t, db, cnr, fs,
		objectcore.AddressOf(raw1),
		objectcore.AddressOf(raw2),
	)

	fs = object.SearchFilters{}
	fs.AddFilter("x", "y", object.MatchStringEqual)
	testSelect(t, db, cnr, fs, objectcore.AddressOf(raw1))

	fs = object.SearchFilters{}
	fs.AddFilter("x", "y", object.MatchStringNotEqual)
	testSelect(t, db, cnr, fs, objectcore.AddressOf(raw2))

	fs = object.SearchFilters{}
	fs.AddFilter("a", "b", object.MatchStringEqual)
	testSelect(t, db, cnr, fs, objectcore.AddressOf(raw3))

	fs = object.SearchFilters{}
	fs.AddFilter("c", "d", object.MatchStringEqual)
	testSelect(t, db, cnr, fs)

	fs = object.SearchFilters{}
	fs.AddFilter("foo", "", object.MatchNotPresent)
	testSelect(t, db, cnr, fs,
		objectcore.AddressOf(raw3),
		objectcore.AddressOf(raw4),
		objectcore.AddressOf(raw5),
		objectcore.AddressOf(raw6),
	)

	fs = object.SearchFilters{}
	fs.AddFilter("a", "", object.MatchNotPresent)
	testSelect(t, db, cnr, fs,
		objectcore.AddressOf(raw1),
		objectcore.AddressOf(raw2),
		objectcore.AddressOf(raw4),
		objectcore.AddressOf(raw5),
		objectcore.AddressOf(raw6),
	)

	fs = object.SearchFilters{}
	testSelect(t, db, cnr, fs,
		objectcore.AddressOf(raw1),
		objectcore.AddressOf(raw2),
		objectcore.AddressOf(raw3),
		objectcore.AddressOf(raw4),
		objectcore.AddressOf(raw5),
		objectcore.AddressOf(raw6),
	)

	fs = object.SearchFilters{}
	fs.AddFilter("key", "", object.MatchNotPresent)
	testSelect(t, db, cnr, fs,
		objectcore.AddressOf(raw1),
		objectcore.AddressOf(raw2),
		objectcore.AddressOf(raw3),
		objectcore.AddressOf(raw4),
		objectcore.AddressOf(raw5),
		objectcore.AddressOf(raw6),
	)

	fs = object.SearchFilters{}
	fs.AddFilter("path", "test", object.MatchCommonPrefix)
	testSelect(t, db, cnr, fs,
		objectcore.AddressOf(raw4),
		objectcore.AddressOf(raw5),
		objectcore.AddressOf(raw6),
	)

	fs = object.SearchFilters{}
	fs.AddFilter("path", "test/1", object.MatchCommonPrefix)
	testSelect(t, db, cnr, fs,
		objectcore.AddressOf(raw4),
		objectcore.AddressOf(raw5),
	)
}

func TestDB_SelectRootPhyParent(t *testing.T) {
	db := newDB(t)

	cnr := cidtest.ID()

	// prepare

	small := generateObjectWithCID(t, cnr)
	err := putBig(db, small)
	require.NoError(t, err)

	ts := generateObjectWithCID(t, cnr)
	ts.SetType(object.TypeTombstone)
	err = putBig(db, ts)
	require.NoError(t, err)

	firstChild := oidtest.ID()

	leftChild := generateObjectWithCID(t, cnr)
	leftChild.SetFirstID(firstChild)
	err = putBig(db, leftChild)
	require.NoError(t, err)

	lock := generateObjectWithCID(t, cnr)
	lock.SetType(object.TypeLock)
	err = putBig(db, lock)
	require.NoError(t, err)

	parent := generateObjectWithCID(t, cnr)

	rightChild := generateObjectWithCID(t, cnr)
	rightChild.SetParent(parent)
	rightChild.SetFirstID(firstChild)
	idParent := parent.GetID()
	rightChild.SetParentID(idParent)
	err = putBig(db, rightChild)
	require.NoError(t, err)

	link := generateObjectWithCID(t, cnr)
	link.SetParent(parent)
	link.SetParentID(idParent)
	link.SetFirstID(firstChild)
	idLeftChild := leftChild.GetID()
	idRightChild := rightChild.GetID()
	link.SetChildren(idLeftChild, idRightChild)

	err = putBig(db, link)
	require.NoError(t, err)

	t.Run("root objects", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddRootFilter()
		testSelect(t, db, cnr, fs,
			objectcore.AddressOf(small),
			objectcore.AddressOf(parent),
		)
	})

	t.Run("phy objects", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddPhyFilter()
		testSelect(t, db, cnr, fs,
			objectcore.AddressOf(small),
			objectcore.AddressOf(ts),
			objectcore.AddressOf(leftChild),
			objectcore.AddressOf(rightChild),
			objectcore.AddressOf(link),
			objectcore.AddressOf(lock),
		)
	})

	t.Run("regular objects", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddTypeFilter(object.MatchStringEqual, object.TypeRegular)
		testSelect(t, db, cnr, fs,
			objectcore.AddressOf(small),
			objectcore.AddressOf(leftChild),
			objectcore.AddressOf(rightChild),
			objectcore.AddressOf(link),
			objectcore.AddressOf(parent),
		)

		fs = object.SearchFilters{}
		fs.AddTypeFilter(object.MatchStringNotEqual, object.TypeRegular)
		testSelect(t, db, cnr, fs,
			objectcore.AddressOf(ts),
			objectcore.AddressOf(lock),
		)

		fs = object.SearchFilters{}
		fs.AddFilter(object.FilterType, "", object.MatchNotPresent)
		testSelect(t, db, cnr, fs)
	})

	t.Run("tombstone objects", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddTypeFilter(object.MatchStringEqual, object.TypeTombstone)
		testSelect(t, db, cnr, fs, objectcore.AddressOf(ts))

		fs = object.SearchFilters{}
		fs.AddTypeFilter(object.MatchStringNotEqual, object.TypeTombstone)
		testSelect(t, db, cnr, fs,
			objectcore.AddressOf(small),
			objectcore.AddressOf(leftChild),
			objectcore.AddressOf(rightChild),
			objectcore.AddressOf(link),
			objectcore.AddressOf(parent),
			objectcore.AddressOf(lock),
		)

		fs = object.SearchFilters{}
		fs.AddFilter(object.FilterType, "", object.MatchNotPresent)
		testSelect(t, db, cnr, fs)
	})

	t.Run("objects with parent", func(t *testing.T) {
		idParent := parent.GetID()

		fs := object.SearchFilters{}
		fs.AddParentIDFilter(object.MatchStringEqual, idParent)

		testSelect(t, db, cnr, fs,
			objectcore.AddressOf(rightChild),
			objectcore.AddressOf(link),
		)

		fs = object.SearchFilters{}
		fs.AddFilter(object.FilterParentID, "", object.MatchNotPresent)
		testSelect(t, db, cnr, fs)

		fs = object.SearchFilters{}
		fs.AddFirstSplitObjectFilter(object.MatchStringEqual, firstChild)

		testSelect(t, db, cnr, fs,
			objectcore.AddressOf(leftChild),
			objectcore.AddressOf(rightChild),
			objectcore.AddressOf(link),
		)
	})

	t.Run("all objects", func(t *testing.T) {
		fs := object.SearchFilters{}
		testSelect(t, db, cnr, fs,
			objectcore.AddressOf(small),
			objectcore.AddressOf(ts),
			objectcore.AddressOf(leftChild),
			objectcore.AddressOf(rightChild),
			objectcore.AddressOf(link),
			objectcore.AddressOf(parent),
			objectcore.AddressOf(lock),
		)
	})
}

func TestDB_SelectInhume(t *testing.T) {
	db := newDB(t)

	cnr := cidtest.ID()

	raw1 := generateObjectWithCID(t, cnr)
	err := putBig(db, raw1)
	require.NoError(t, err)

	raw2 := generateObjectWithCID(t, cnr)
	err = putBig(db, raw2)
	require.NoError(t, err)

	fs := object.SearchFilters{}
	testSelect(t, db, cnr, fs,
		objectcore.AddressOf(raw1),
		objectcore.AddressOf(raw2),
	)

	var ts = createTSForObject(cnr, raw2.GetID())
	require.NoError(t, db.Put(ts))

	fs = object.SearchFilters{}
	testSelect(t, db, cnr, fs,
		objectcore.AddressOf(raw1),
		objectcore.AddressOf(ts),
	)
}

func TestDB_SelectPayloadHash(t *testing.T) {
	db := newDB(t)

	cnr := cidtest.ID()

	raw1 := generateObjectWithCID(t, cnr)
	err := putBig(db, raw1)
	require.NoError(t, err)

	raw2 := generateObjectWithCID(t, cnr)
	err = putBig(db, raw2)
	require.NoError(t, err)

	cs, _ := raw1.PayloadChecksum()
	payloadHash := [sha256.Size]byte(cs.Value())

	t.Run("equal filter", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddPayloadHashFilter(object.MatchStringEqual, payloadHash)

		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw1))
	})

	t.Run("common prefix filter", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddFilter(object.FilterPayloadChecksum,
			hex.EncodeToString(payloadHash[:len(payloadHash)-1]),
			object.MatchCommonPrefix)

		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw1))
	})

	t.Run("not equal filter", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddPayloadHashFilter(object.MatchStringNotEqual, payloadHash)

		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw2))
	})

	t.Run("not present filter", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddFilter(object.FilterPayloadChecksum,
			"",
			object.MatchNotPresent)

		testSelect(t, db, cnr, fs)
	})

	t.Run("invalid hashes", func(t *testing.T) {
		fs := object.SearchFilters{}
		otherHash := payloadHash
		otherHash[0]++
		fs.AddPayloadHashFilter(object.MatchStringNotEqual, otherHash)

		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw1), objectcore.AddressOf(raw2))

		fs = object.SearchFilters{}
		fs.AddPayloadHashFilter(object.MatchCommonPrefix, otherHash)

		testSelect(t, db, cnr, fs)
	})
}

func TestDB_SelectWithSlowFilters(t *testing.T) {
	db := newDB(t)

	cnr := cidtest.ID()

	v20 := new(version.Version)
	v20.SetMajor(2)

	var v21 version.Version
	v21.SetMajor(2)
	v21.SetMinor(1)

	raw1 := generateObjectWithCID(t, cnr)
	raw1.SetPayloadSize(10)
	raw1.SetCreationEpoch(11)
	raw1.SetVersion(v20)
	err := putBig(db, raw1)
	require.NoError(t, err)

	raw2 := generateObjectWithCID(t, cnr)
	raw2.SetPayloadSize(20)
	raw2.SetCreationEpoch(21)
	raw2.SetVersion(&v21)
	err = putBig(db, raw2)
	require.NoError(t, err)

	t.Run("object with TZHash", func(t *testing.T) {
		cs, _ := raw1.PayloadHomomorphicHash()
		homoHash := [tz.Size]byte(cs.Value())

		fs := object.SearchFilters{}
		fs.AddHomomorphicHashFilter(object.MatchStringEqual, homoHash)

		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw1))

		fs = object.SearchFilters{}
		fs.AddHomomorphicHashFilter(object.MatchStringNotEqual, homoHash)

		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw2))

		fs = object.SearchFilters{}
		fs.AddFilter(object.FilterPayloadHomomorphicHash,
			"",
			object.MatchNotPresent)

		testSelect(t, db, cnr, fs)
	})

	t.Run("object with payload length", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddPayloadSizeFilter(object.MatchStringEqual, raw2.PayloadSize())

		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw2))

		fs = object.SearchFilters{}
		fs.AddPayloadSizeFilter(object.MatchStringNotEqual, raw2.PayloadSize())

		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw1))

		fs = object.SearchFilters{}
		fs.AddFilter(object.FilterPayloadSize, "", object.MatchNotPresent)

		testSelect(t, db, cnr, fs)
	})

	t.Run("object with creation epoch", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddCreationEpochFilter(object.MatchStringEqual, raw1.CreationEpoch())

		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw1))

		fs = object.SearchFilters{}
		fs.AddCreationEpochFilter(object.MatchStringNotEqual, raw1.CreationEpoch())

		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw2))

		fs = object.SearchFilters{}
		fs.AddFilter(object.FilterCreationEpoch, "", object.MatchNotPresent)

		testSelect(t, db, cnr, fs)

		fs = object.SearchFilters{}
		fs.AddCreationEpochFilter(object.MatchCommonPrefix, 1)

		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw1))
	})

	t.Run("object with version", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddObjectVersionFilter(object.MatchStringEqual, v21)
		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw2))

		fs = object.SearchFilters{}
		fs.AddObjectVersionFilter(object.MatchStringNotEqual, v21)
		testSelect(t, db, cnr, fs, objectcore.AddressOf(raw1))

		fs = object.SearchFilters{}
		fs.AddObjectVersionFilter(object.MatchNotPresent, version.Version{})
		testSelect(t, db, cnr, fs)
	})
}

func TestDB_SelectSplitID(t *testing.T) {
	db := newDB(t)

	cnr := cidtest.ID()

	child1 := generateObjectWithCID(t, cnr)
	child2 := generateObjectWithCID(t, cnr)
	child3 := generateObjectWithCID(t, cnr)

	split1 := object.NewSplitID()
	split2 := object.NewSplitID()

	child1.SetSplitID(split1)
	child2.SetSplitID(split1)
	child3.SetSplitID(split2)

	require.NoError(t, putBig(db, child1))
	require.NoError(t, putBig(db, child2))
	require.NoError(t, putBig(db, child3))

	t.Run("not present", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddFilter(object.FilterSplitID, "", object.MatchNotPresent)
		testSelect(t, db, cnr, fs)
	})

	t.Run("split id", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddSplitIDFilter(object.MatchStringEqual, *split1)
		testSelect(t, db, cnr, fs,
			objectcore.AddressOf(child1),
			objectcore.AddressOf(child2),
		)

		fs = object.SearchFilters{}
		fs.AddSplitIDFilter(object.MatchStringEqual, *split2)
		testSelect(t, db, cnr, fs, objectcore.AddressOf(child3))
	})

	t.Run("unknown split id", func(t *testing.T) {
		fs := object.SearchFilters{}
		fs.AddSplitIDFilter(object.MatchStringEqual, *object.NewSplitID())
		testSelect(t, db, cnr, fs)
	})
}

func BenchmarkSelect(b *testing.B) {
	const objCount = 1000
	db := newDB(b)
	cid := cidtest.ID()

	for i := range objCount {
		var attr object.Attribute
		attr.SetKey("myHeader")
		attr.SetValue(strconv.Itoa(i))
		obj := generateObjectWithCID(b, cid)
		obj.SetAttributes(attr)
		require.NoError(b, metaPut(db, obj))
	}

	b.Run("string equal", func(b *testing.B) {
		fs := object.SearchFilters{}
		fs.AddFilter("myHeader", strconv.Itoa(objCount/2), object.MatchStringEqual)
		benchmarkSelect(b, db, cid, fs, 1)
	})
	b.Run("string not equal", func(b *testing.B) {
		fs := object.SearchFilters{}
		fs.AddFilter("myHeader", strconv.Itoa(objCount/2), object.MatchStringNotEqual)
		benchmarkSelect(b, db, cid, fs, objCount-1)
	})
	b.Run("common prefix", func(b *testing.B) {
		prefix := "99"
		n := 1 /* 99 */ + 10 /* 990..999 */

		fs := object.SearchFilters{}
		fs.AddFilter("myHeader", prefix, object.MatchCommonPrefix)
		benchmarkSelect(b, db, cid, fs, n)
	})
	b.Run("unknown", func(b *testing.B) {
		fs := object.SearchFilters{}
		fs.AddFilter("myHeader", strconv.Itoa(objCount/2), object.MatchUnspecified)
		benchmarkSelect(b, db, cid, fs, 0)
	})
}

func TestExpiredObjects(t *testing.T) {
	db := newDB(t, meta.WithEpochState(epochState{currEpoch}))

	checkExpiredObjects(t, db, func(exp, nonExp *object.Object) {
		cidExp := exp.GetContainerID()
		cidNonExp := nonExp.GetContainerID()

		objs, err := metaSelect(db, cidExp, object.SearchFilters{})
		require.NoError(t, err)
		require.Empty(t, objs) // expired object should not be returned

		objs, err = metaSelect(db, cidNonExp, object.SearchFilters{})
		require.NoError(t, err)
		require.NotEmpty(t, objs)
	})
}

func TestRemovedObjects(t *testing.T) {
	db := newDB(t, meta.WithEpochState(epochState{currEpoch}))

	cnr := cidtest.ID()

	o1 := generateObjectWithCID(t, cnr)
	addAttribute(o1, "1", "11")

	o2 := generateObjectWithCID(t, cnr)
	addAttribute(o2, "2", "22")

	o3 := generateObjectWithCID(t, cnr) // expired but will be locked
	setExpiration(o3, currEpoch-1)

	require.NoError(t, putBig(db, o1))
	require.NoError(t, putBig(db, o2))
	require.NoError(t, putBig(db, o3))

	f1 := object.SearchFilters{}
	f1.AddFilter("1", "11", object.MatchStringEqual)

	f2 := object.SearchFilters{}
	f2.AddFilter("2", "22", object.MatchStringEqual)

	fAll := object.SearchFilters{}

	testSelect(t, db, cnr, f1, objectcore.AddressOf(o1))
	testSelect(t, db, cnr, f2, objectcore.AddressOf(o2))
	testSelect(t, db, cnr, fAll, objectcore.AddressOf(o1), objectcore.AddressOf(o2))

	// Removed object

	ts1 := createTSForObject(o1.GetContainerID(), o1.GetID())
	require.NoError(t, db.Put(ts1))

	oo, err := metaSelect(db, cnr, f1)
	require.NoError(t, err)
	require.Empty(t, oo)

	testSelect(t, db, cnr, fAll, objectcore.AddressOf(o2), objectcore.AddressOf(ts1))

	// Expired (== removed) but locked

	locker := generateObjectWithCID(t, objectcore.AddressOf(o3).Container())
	locker.AssociateLocked(objectcore.AddressOf(o3).Object())
	require.NoError(t, db.Put(locker))

	testSelect(t, db, cnr, fAll, objectcore.AddressOf(o2), objectcore.AddressOf(ts1), objectcore.AddressOf(o3), objectcore.AddressOf(locker))
}

func benchmarkSelect(b *testing.B, db *meta.DB, cid cidSDK.ID, fs object.SearchFilters, expected int) {
	for b.Loop() {
		addrs, err := db.Select(cid, fs)
		if err != nil {
			b.Fatal(err)
		}
		if len(addrs) != expected {
			b.Fatalf("expected %d items, got %d", expected, len(addrs))
		}
	}
}

func metaSelect(db *meta.DB, cnr cidSDK.ID, fs object.SearchFilters) ([]oid.Address, error) {
	return db.Select(cnr, fs)
}

func numQuery(key string, op object.SearchMatchType, val string) (res object.SearchFilters) {
	res.AddFilter(key, val, op)
	return
}

var allNumOps = []object.SearchMatchType{
	object.MatchNumGT,
	object.MatchNumGE,
	object.MatchNumLT,
	object.MatchNumLE,
}

func TestNumericSelect(t *testing.T) {
	db := newDB(t)
	cnr := cidtest.ID()

	for i, testAttr := range []struct {
		key string
		set func(obj *object.Object, val uint64)
	}{
		{key: "$Object:creationEpoch", set: func(obj *object.Object, val uint64) { obj.SetCreationEpoch(val) }},
		{key: "$Object:payloadLength", set: func(obj *object.Object, val uint64) { obj.SetPayloadSize(val) }},
		{key: "any_user_attr", set: func(obj *object.Object, val uint64) {
			addAttribute(obj, "any_user_attr", strconv.FormatUint(val, 10))
		}},
	} {
		cnr := cidtest.ID()
		obj1 := generateObjectWithCID(t, cnr)
		addr1 := objectcore.AddressOf(obj1)
		obj2 := generateObjectWithCID(t, cnr)
		addr2 := objectcore.AddressOf(obj2)

		const smallNum = 10
		const midNum = 11
		const bigNum = 12

		testAttr.set(obj1, smallNum)
		testAttr.set(obj2, bigNum)

		require.NoError(t, putBig(db, obj1), i)
		require.NoError(t, putBig(db, obj2), i)

		for j, testCase := range []struct {
			op  object.SearchMatchType
			num uint64
			exp []oid.Address
		}{
			{op: object.MatchNumLT, num: smallNum - 1, exp: nil},
			{op: object.MatchNumLT, num: smallNum, exp: nil},
			{op: object.MatchNumLT, num: midNum, exp: []oid.Address{addr1}},
			{op: object.MatchNumLT, num: bigNum, exp: []oid.Address{addr1}},
			{op: object.MatchNumLT, num: bigNum + 1, exp: []oid.Address{addr1, addr2}},

			{op: object.MatchNumLE, num: smallNum - 1, exp: nil},
			{op: object.MatchNumLE, num: smallNum, exp: []oid.Address{addr1}},
			{op: object.MatchNumLE, num: midNum, exp: []oid.Address{addr1}},
			{op: object.MatchNumLE, num: bigNum, exp: []oid.Address{addr1, addr2}},
			{op: object.MatchNumLE, num: bigNum + 1, exp: []oid.Address{addr1, addr2}},

			{op: object.MatchNumGE, num: smallNum - 1, exp: []oid.Address{addr1, addr2}},
			{op: object.MatchNumGE, num: smallNum, exp: []oid.Address{addr1, addr2}},
			{op: object.MatchNumGE, num: midNum, exp: []oid.Address{addr2}},
			{op: object.MatchNumGE, num: bigNum, exp: []oid.Address{addr2}},
			{op: object.MatchNumGE, num: bigNum + 1, exp: nil},

			{op: object.MatchNumGT, num: smallNum - 1, exp: []oid.Address{addr1, addr2}},
			{op: object.MatchNumGT, num: smallNum, exp: []oid.Address{addr2}},
			{op: object.MatchNumGT, num: midNum, exp: []oid.Address{addr2}},
			{op: object.MatchNumGT, num: bigNum, exp: nil},
			{op: object.MatchNumGT, num: bigNum + 1, exp: nil},
		} {
			query := numQuery(testAttr.key, testCase.op, strconv.FormatUint(testCase.num, 10))

			res, err := metaSelect(db, cnr, query)
			require.NoError(t, err, [2]any{i, j})
			require.Len(t, res, len(testCase.exp), [2]any{i, j})

			for i := range testCase.exp {
				require.Contains(t, res, testCase.exp[i], [2]any{i, j})
			}
		}
	}

	// negative values
	cnrNeg := cidtest.ID()
	obj = generateObjectWithCID(t, cnrNeg)
	const objVal = int64(-10)
	const negAttr = "negative_attr"
	addAttribute(obj, negAttr, strconv.FormatInt(objVal, 10))
	addr := objectcore.AddressOf(obj)

	require.NoError(t, putBig(db, obj))

	val := objVal - 1
	testSelect(t, db, cnrNeg, numQuery(negAttr, object.MatchNumGT, strconv.FormatInt(val, 10)), addr)
	testSelect(t, db, cnrNeg, numQuery(negAttr, object.MatchNumGE, strconv.FormatInt(val, 10)), addr)
	testSelect(t, db, cnrNeg, numQuery(negAttr, object.MatchNumLT, strconv.FormatInt(val, 10)))
	testSelect(t, db, cnrNeg, numQuery(negAttr, object.MatchNumLE, strconv.FormatInt(val, 10)))
	val = objVal
	testSelect(t, db, cnrNeg, numQuery(negAttr, object.MatchNumGT, strconv.FormatInt(val, 10)))
	testSelect(t, db, cnrNeg, numQuery(negAttr, object.MatchNumGE, strconv.FormatInt(val, 10)), addr)
	testSelect(t, db, cnrNeg, numQuery(negAttr, object.MatchNumLT, strconv.FormatInt(val, 10)))
	testSelect(t, db, cnrNeg, numQuery(negAttr, object.MatchNumLE, strconv.FormatInt(val, 10)), addr)
	val = objVal + 1
	testSelect(t, db, cnrNeg, numQuery(negAttr, object.MatchNumGT, strconv.FormatInt(val, 10)))
	testSelect(t, db, cnrNeg, numQuery(negAttr, object.MatchNumGE, strconv.FormatInt(val, 10)))
	testSelect(t, db, cnrNeg, numQuery(negAttr, object.MatchNumLT, strconv.FormatInt(val, 10)), addr)
	testSelect(t, db, cnrNeg, numQuery(negAttr, object.MatchNumLE, strconv.FormatInt(val, 10)), addr)

	// uint64 overflow
	for _, attr := range []string{
		"$Object:creationEpoch",
		"$Object:payloadLength",
	} {
		b := new(big.Int).SetUint64(math.MaxUint64)
		b.Add(b, big.NewInt(1))
		filterVal := b.String()

		testSelect(t, db, cnrNeg, numQuery(attr, object.MatchNumGT, filterVal))
		testSelect(t, db, cnrNeg, numQuery(attr, object.MatchNumGE, filterVal))
		testSelect(t, db, cnrNeg, numQuery(attr, object.MatchNumLT, filterVal), addr)
		testSelect(t, db, cnrNeg, numQuery(attr, object.MatchNumLE, filterVal), addr)
	}

	t.Run("invalid filtering value format", func(t *testing.T) {
		for _, op := range allNumOps {
			query := numQuery("any_key", op, "1.0")

			_, err := metaSelect(db, cnr, query)
			require.ErrorIs(t, err, objectcore.ErrInvalidSearchQuery, op)
		}
	})

	t.Run("unsupported system attribute", func(t *testing.T) {
		for _, op := range allNumOps {
			query := numQuery("$Object:definitelyUnknown", op, "123")

			res, err := metaSelect(db, cnr, query)
			require.NoError(t, err, op)
			require.Empty(t, res, op)
		}
	})

	t.Run("non-numeric user attributes", func(t *testing.T) {
		// unlike system attributes, the user cannot always guarantee that the filtered
		// attribute will be correct in all objects. This should not cause a denial of
		// service, so such objects are simply not included in the result
		obj1 := generateObjectWithCID(t, cnr)
		addr1 := objectcore.AddressOf(obj1)
		obj2 := generateObjectWithCID(t, cnr)
		addr2 := objectcore.AddressOf(obj2)

		const attr = "any_user_attribute"
		addAttribute(obj1, attr, "123")
		addAttribute(obj2, attr, "any_non_num")

		require.NoError(t, putBig(db, obj1))
		require.NoError(t, putBig(db, obj2))

		var query object.SearchFilters

		testSelect(t, db, cnr, query,
			addr1, addr2)

		for _, testCase := range []struct {
			op  object.SearchMatchType
			val string
		}{
			{op: object.MatchNumGT, val: "122"},
			{op: object.MatchNumGE, val: "122"},
			{op: object.MatchNumGE, val: "123"},
			{op: object.MatchNumLE, val: "123"},
			{op: object.MatchNumLE, val: "124"},
			{op: object.MatchNumLT, val: "124"},
		} {
			query = numQuery(attr, testCase.op, testCase.val)

			res, err := metaSelect(db, cnr, query)
			require.NoError(t, err, testCase)
			require.Equal(t, []oid.Address{addr1}, res, testCase)
		}
	})
}
