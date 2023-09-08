package meta_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
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

	fs := objectSDK.SearchFilters{}
	fs.AddFilter("foo", "bar", objectSDK.MatchStringEqual)
	testSelect(t, db, cnr, fs,
		object.AddressOf(raw1),
		object.AddressOf(raw2),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("x", "y", objectSDK.MatchStringEqual)
	testSelect(t, db, cnr, fs, object.AddressOf(raw1))

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("x", "y", objectSDK.MatchStringNotEqual)
	testSelect(t, db, cnr, fs, object.AddressOf(raw2))

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("a", "b", objectSDK.MatchStringEqual)
	testSelect(t, db, cnr, fs, object.AddressOf(raw3))

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("c", "d", objectSDK.MatchStringEqual)
	testSelect(t, db, cnr, fs)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("foo", "", objectSDK.MatchNotPresent)
	testSelect(t, db, cnr, fs,
		object.AddressOf(raw3),
		object.AddressOf(raw4),
		object.AddressOf(raw5),
		object.AddressOf(raw6),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("a", "", objectSDK.MatchNotPresent)
	testSelect(t, db, cnr, fs,
		object.AddressOf(raw1),
		object.AddressOf(raw2),
		object.AddressOf(raw4),
		object.AddressOf(raw5),
		object.AddressOf(raw6),
	)

	fs = objectSDK.SearchFilters{}
	testSelect(t, db, cnr, fs,
		object.AddressOf(raw1),
		object.AddressOf(raw2),
		object.AddressOf(raw3),
		object.AddressOf(raw4),
		object.AddressOf(raw5),
		object.AddressOf(raw6),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("key", "", objectSDK.MatchNotPresent)
	testSelect(t, db, cnr, fs,
		object.AddressOf(raw1),
		object.AddressOf(raw2),
		object.AddressOf(raw3),
		object.AddressOf(raw4),
		object.AddressOf(raw5),
		object.AddressOf(raw6),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("path", "test", objectSDK.MatchCommonPrefix)
	testSelect(t, db, cnr, fs,
		object.AddressOf(raw4),
		object.AddressOf(raw5),
		object.AddressOf(raw6),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("path", "test/1", objectSDK.MatchCommonPrefix)
	testSelect(t, db, cnr, fs,
		object.AddressOf(raw4),
		object.AddressOf(raw5),
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
	ts.SetType(objectSDK.TypeTombstone)
	err = putBig(db, ts)
	require.NoError(t, err)

	sg := generateObjectWithCID(t, cnr)
	sg.SetType(objectSDK.TypeStorageGroup)
	err = putBig(db, sg)
	require.NoError(t, err)

	leftChild := generateObjectWithCID(t, cnr)
	leftChild.InitRelations()
	err = putBig(db, leftChild)
	require.NoError(t, err)

	lock := generateObjectWithCID(t, cnr)
	lock.SetType(objectSDK.TypeLock)
	err = putBig(db, lock)
	require.NoError(t, err)

	parent := generateObjectWithCID(t, cnr)

	rightChild := generateObjectWithCID(t, cnr)
	rightChild.SetParent(parent)
	idParent, _ := parent.ID()
	rightChild.SetParentID(idParent)
	err = putBig(db, rightChild)
	require.NoError(t, err)

	link := generateObjectWithCID(t, cnr)
	link.SetParent(parent)
	link.SetParentID(idParent)
	idLeftChild, _ := leftChild.ID()
	idRightChild, _ := rightChild.ID()
	link.SetChildren(idLeftChild, idRightChild)

	err = putBig(db, link)
	require.NoError(t, err)

	t.Run("root objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddRootFilter()
		testSelect(t, db, cnr, fs,
			object.AddressOf(small),
			object.AddressOf(parent),
		)
	})

	t.Run("phy objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddPhyFilter()
		testSelect(t, db, cnr, fs,
			object.AddressOf(small),
			object.AddressOf(ts),
			object.AddressOf(sg),
			object.AddressOf(leftChild),
			object.AddressOf(rightChild),
			object.AddressOf(link),
			object.AddressOf(lock),
		)
	})

	t.Run("regular objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddTypeFilter(objectSDK.MatchStringEqual, objectSDK.TypeRegular)
		testSelect(t, db, cnr, fs,
			object.AddressOf(small),
			object.AddressOf(leftChild),
			object.AddressOf(rightChild),
			object.AddressOf(link),
			object.AddressOf(parent),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddTypeFilter(objectSDK.MatchStringNotEqual, objectSDK.TypeRegular)
		testSelect(t, db, cnr, fs,
			object.AddressOf(ts),
			object.AddressOf(sg),
			object.AddressOf(lock),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(objectSDK.FilterType, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cnr, fs)
	})

	t.Run("tombstone objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddTypeFilter(objectSDK.MatchStringEqual, objectSDK.TypeTombstone)
		testSelect(t, db, cnr, fs, object.AddressOf(ts))

		fs = objectSDK.SearchFilters{}
		fs.AddTypeFilter(objectSDK.MatchStringNotEqual, objectSDK.TypeTombstone)
		testSelect(t, db, cnr, fs,
			object.AddressOf(small),
			object.AddressOf(leftChild),
			object.AddressOf(rightChild),
			object.AddressOf(link),
			object.AddressOf(parent),
			object.AddressOf(sg),
			object.AddressOf(lock),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(objectSDK.FilterType, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cnr, fs)
	})

	t.Run("storage group objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddTypeFilter(objectSDK.MatchStringEqual, objectSDK.TypeStorageGroup)
		testSelect(t, db, cnr, fs, object.AddressOf(sg))

		fs = objectSDK.SearchFilters{}
		fs.AddTypeFilter(objectSDK.MatchStringNotEqual, objectSDK.TypeStorageGroup)
		testSelect(t, db, cnr, fs,
			object.AddressOf(small),
			object.AddressOf(leftChild),
			object.AddressOf(rightChild),
			object.AddressOf(link),
			object.AddressOf(parent),
			object.AddressOf(ts),
			object.AddressOf(lock),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(objectSDK.FilterType, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cnr, fs)
	})

	t.Run("objects with parent", func(t *testing.T) {
		idParent, _ := parent.ID()

		fs := objectSDK.SearchFilters{}
		fs.AddParentIDFilter(objectSDK.MatchStringEqual, idParent)

		testSelect(t, db, cnr, fs,
			object.AddressOf(rightChild),
			object.AddressOf(link),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(objectSDK.FilterParentID, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cnr, fs)
	})

	t.Run("all objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		testSelect(t, db, cnr, fs,
			object.AddressOf(small),
			object.AddressOf(ts),
			object.AddressOf(sg),
			object.AddressOf(leftChild),
			object.AddressOf(rightChild),
			object.AddressOf(link),
			object.AddressOf(parent),
			object.AddressOf(lock),
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

	fs := objectSDK.SearchFilters{}
	testSelect(t, db, cnr, fs,
		object.AddressOf(raw1),
		object.AddressOf(raw2),
	)

	var tombstone oid.Address
	tombstone.SetContainer(cnr)
	tombstone.SetObject(oidtest.ID())

	err = metaInhume(db, object.AddressOf(raw2), tombstone)
	require.NoError(t, err)

	fs = objectSDK.SearchFilters{}
	testSelect(t, db, cnr, fs,
		object.AddressOf(raw1),
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
	var payloadHash [sha256.Size]byte
	copy(payloadHash[:], cs.Value())

	fs := objectSDK.SearchFilters{}
	fs.AddPayloadHashFilter(objectSDK.MatchStringEqual, payloadHash)

	testSelect(t, db, cnr, fs, object.AddressOf(raw1))

	fs = objectSDK.SearchFilters{}
	fs.AddFilter(objectSDK.FilterPayloadChecksum,
		hex.EncodeToString(payloadHash[:len(payloadHash)-1]),
		objectSDK.MatchCommonPrefix)

	testSelect(t, db, cnr, fs, object.AddressOf(raw1))

	fs = objectSDK.SearchFilters{}
	fs.AddPayloadHashFilter(objectSDK.MatchStringNotEqual, payloadHash)

	testSelect(t, db, cnr, fs, object.AddressOf(raw2))

	fs = objectSDK.SearchFilters{}
	fs.AddFilter(objectSDK.FilterPayloadChecksum,
		"",
		objectSDK.MatchNotPresent)

	testSelect(t, db, cnr, fs)

	t.Run("invalid hashes", func(t *testing.T) {
		fs = objectSDK.SearchFilters{}
		otherHash := payloadHash
		otherHash[0]++
		fs.AddPayloadHashFilter(objectSDK.MatchStringNotEqual, otherHash)

		testSelect(t, db, cnr, fs, object.AddressOf(raw1), object.AddressOf(raw2))

		fs = objectSDK.SearchFilters{}
		fs.AddPayloadHashFilter(objectSDK.MatchCommonPrefix, otherHash)

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
		var homoHash [tz.Size]byte
		copy(homoHash[:], cs.Value())

		fs := objectSDK.SearchFilters{}
		fs.AddHomomorphicHashFilter(objectSDK.MatchStringEqual, homoHash)

		testSelect(t, db, cnr, fs, object.AddressOf(raw1))

		fs = objectSDK.SearchFilters{}
		fs.AddHomomorphicHashFilter(objectSDK.MatchStringNotEqual, homoHash)

		testSelect(t, db, cnr, fs, object.AddressOf(raw2))

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(objectSDK.FilterPayloadHomomorphicHash,
			"",
			objectSDK.MatchNotPresent)

		testSelect(t, db, cnr, fs)
	})

	t.Run("object with payload length", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddPayloadSizeFilter(objectSDK.MatchStringEqual, raw2.PayloadSize())

		testSelect(t, db, cnr, fs, object.AddressOf(raw2))

		fs = objectSDK.SearchFilters{}
		fs.AddPayloadSizeFilter(objectSDK.MatchStringNotEqual, raw2.PayloadSize())

		testSelect(t, db, cnr, fs, object.AddressOf(raw1))

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(objectSDK.FilterPayloadSize, "", objectSDK.MatchNotPresent)

		testSelect(t, db, cnr, fs)
	})

	t.Run("object with creation epoch", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddCreationEpochFilter(objectSDK.MatchStringEqual, raw1.CreationEpoch())

		testSelect(t, db, cnr, fs, object.AddressOf(raw1))

		fs = objectSDK.SearchFilters{}
		fs.AddCreationEpochFilter(objectSDK.MatchStringNotEqual, raw1.CreationEpoch())

		testSelect(t, db, cnr, fs, object.AddressOf(raw2))

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(objectSDK.FilterCreationEpoch, "", objectSDK.MatchNotPresent)

		testSelect(t, db, cnr, fs)

		fs = objectSDK.SearchFilters{}
		fs.AddCreationEpochFilter(objectSDK.MatchCommonPrefix, 1)

		testSelect(t, db, cnr, fs, object.AddressOf(raw1))
	})

	t.Run("object with version", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectVersionFilter(objectSDK.MatchStringEqual, v21)
		testSelect(t, db, cnr, fs, object.AddressOf(raw2))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectVersionFilter(objectSDK.MatchStringNotEqual, v21)
		testSelect(t, db, cnr, fs, object.AddressOf(raw1))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectVersionFilter(objectSDK.MatchNotPresent, version.Version{})
		testSelect(t, db, cnr, fs)
	})
}

func TestDB_SelectObjectID(t *testing.T) {
	db := newDB(t)

	cnr := cidtest.ID()

	// prepare

	parent := generateObjectWithCID(t, cnr)

	regular := generateObjectWithCID(t, cnr)
	idParent, _ := parent.ID()
	regular.SetParentID(idParent)
	regular.SetParent(parent)

	err := putBig(db, regular)
	require.NoError(t, err)

	ts := generateObjectWithCID(t, cnr)
	ts.SetType(objectSDK.TypeTombstone)
	err = putBig(db, ts)
	require.NoError(t, err)

	sg := generateObjectWithCID(t, cnr)
	sg.SetType(objectSDK.TypeStorageGroup)
	err = putBig(db, sg)
	require.NoError(t, err)

	lock := generateObjectWithCID(t, cnr)
	lock.SetType(objectSDK.TypeLock)
	err = putBig(db, lock)
	require.NoError(t, err)

	t.Run("not found objects", func(t *testing.T) {
		raw := generateObjectWithCID(t, cnr)

		id, _ := raw.ID()

		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, id)

		testSelect(t, db, cnr, fs)

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, id)

		testSelect(t, db, cnr, fs,
			object.AddressOf(regular),
			object.AddressOf(parent),
			object.AddressOf(sg),
			object.AddressOf(ts),
			object.AddressOf(lock),
		)
	})

	t.Run("regular objects", func(t *testing.T) {
		id, _ := regular.ID()

		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, id)
		testSelect(t, db, cnr, fs, object.AddressOf(regular))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, id)
		testSelect(t, db, cnr, fs,
			object.AddressOf(parent),
			object.AddressOf(sg),
			object.AddressOf(ts),
			object.AddressOf(lock),
		)
	})

	t.Run("tombstone objects", func(t *testing.T) {
		id, _ := ts.ID()

		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, id)
		testSelect(t, db, cnr, fs, object.AddressOf(ts))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, id)
		testSelect(t, db, cnr, fs,
			object.AddressOf(regular),
			object.AddressOf(parent),
			object.AddressOf(sg),
			object.AddressOf(lock),
		)
	})

	t.Run("storage group objects", func(t *testing.T) {
		id, _ := sg.ID()

		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, id)
		testSelect(t, db, cnr, fs, object.AddressOf(sg))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, id)
		testSelect(t, db, cnr, fs,
			object.AddressOf(regular),
			object.AddressOf(parent),
			object.AddressOf(ts),
			object.AddressOf(lock),
		)
	})

	t.Run("parent objects", func(t *testing.T) {
		id, _ := parent.ID()

		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, id)
		testSelect(t, db, cnr, fs, object.AddressOf(parent))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, id)
		testSelect(t, db, cnr, fs,
			object.AddressOf(regular),
			object.AddressOf(sg),
			object.AddressOf(ts),
			object.AddressOf(lock),
		)
	})

	t.Run("lock objects", func(t *testing.T) {
		id, _ := lock.ID()

		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, id)
		testSelect(t, db, cnr, fs, object.AddressOf(lock))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, id)
		testSelect(t, db, cnr, fs,
			object.AddressOf(regular),
			object.AddressOf(parent),
			object.AddressOf(sg),
			object.AddressOf(ts),
		)
	})
}

func TestDB_SelectSplitID(t *testing.T) {
	db := newDB(t)

	cnr := cidtest.ID()

	child1 := generateObjectWithCID(t, cnr)
	child2 := generateObjectWithCID(t, cnr)
	child3 := generateObjectWithCID(t, cnr)

	split1 := objectSDK.NewSplitID()
	split2 := objectSDK.NewSplitID()

	child1.SetSplitID(split1)
	child2.SetSplitID(split1)
	child3.SetSplitID(split2)

	require.NoError(t, putBig(db, child1))
	require.NoError(t, putBig(db, child2))
	require.NoError(t, putBig(db, child3))

	t.Run("not present", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(objectSDK.FilterSplitID, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cnr, fs)
	})

	t.Run("split id", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddSplitIDFilter(objectSDK.MatchStringEqual, *split1)
		testSelect(t, db, cnr, fs,
			object.AddressOf(child1),
			object.AddressOf(child2),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddSplitIDFilter(objectSDK.MatchStringEqual, *split2)
		testSelect(t, db, cnr, fs, object.AddressOf(child3))
	})

	t.Run("unknown split id", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddSplitIDFilter(objectSDK.MatchStringEqual, *objectSDK.NewSplitID())
		testSelect(t, db, cnr, fs)
	})
}

func TestDB_SelectContainerID(t *testing.T) {
	db := newDB(t)

	cnr := cidtest.ID()

	obj1 := generateObjectWithCID(t, cnr)
	err := putBig(db, obj1)
	require.NoError(t, err)

	obj2 := generateObjectWithCID(t, cnr)
	err = putBig(db, obj2)
	require.NoError(t, err)

	t.Run("same cid", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectContainerIDFilter(objectSDK.MatchStringEqual, cnr)

		testSelect(t, db, cnr, fs,
			object.AddressOf(obj1),
			object.AddressOf(obj2),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddObjectContainerIDFilter(objectSDK.MatchStringNotEqual, cnr)

		testSelect(t, db, cnr, fs,
			object.AddressOf(obj1),
			object.AddressOf(obj2),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddObjectContainerIDFilter(objectSDK.MatchNotPresent, cnr)

		testSelect(t, db, cnr, fs)
	})

	t.Run("not same cid", func(t *testing.T) {
		newCnr := cidtest.ID()

		fs := objectSDK.SearchFilters{}
		fs.AddObjectContainerIDFilter(objectSDK.MatchStringEqual, newCnr)

		testSelect(t, db, cnr, fs)
	})
}

func BenchmarkSelect(b *testing.B) {
	const objCount = 1000
	db := newDB(b)
	cid := cidtest.ID()

	for i := 0; i < objCount; i++ {
		var attr objectSDK.Attribute
		attr.SetKey("myHeader")
		attr.SetValue(strconv.Itoa(i))
		obj := generateObjectWithCID(b, cid)
		obj.SetAttributes(attr)
		require.NoError(b, metaPut(db, obj, nil))
	}

	b.Run("string equal", func(b *testing.B) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter("myHeader", strconv.Itoa(objCount/2), objectSDK.MatchStringEqual)
		benchmarkSelect(b, db, cid, fs, 1)
	})
	b.Run("string not equal", func(b *testing.B) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter("myHeader", strconv.Itoa(objCount/2), objectSDK.MatchStringNotEqual)
		benchmarkSelect(b, db, cid, fs, objCount-1)
	})
	b.Run("common prefix", func(b *testing.B) {
		prefix := "99"
		n := 1 /* 99 */ + 10 /* 990..999 */

		fs := objectSDK.SearchFilters{}
		fs.AddFilter("myHeader", prefix, objectSDK.MatchCommonPrefix)
		benchmarkSelect(b, db, cid, fs, n)
	})
	b.Run("unknown", func(b *testing.B) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter("myHeader", strconv.Itoa(objCount/2), objectSDK.MatchUnknown)
		benchmarkSelect(b, db, cid, fs, 0)
	})
}

func TestExpiredObjects(t *testing.T) {
	db := newDB(t, meta.WithEpochState(epochState{currEpoch}))

	checkExpiredObjects(t, db, func(exp, nonExp *objectSDK.Object) {
		cidExp, _ := exp.ContainerID()
		cidNonExp, _ := nonExp.ContainerID()

		objs, err := metaSelect(db, cidExp, objectSDK.SearchFilters{})
		require.NoError(t, err)
		require.Empty(t, objs) // expired object should not be returned

		objs, err = metaSelect(db, cidNonExp, objectSDK.SearchFilters{})
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

	f1 := objectSDK.SearchFilters{}
	f1.AddFilter("1", "11", objectSDK.MatchStringEqual)

	f2 := objectSDK.SearchFilters{}
	f2.AddFilter("2", "22", objectSDK.MatchStringEqual)

	fAll := objectSDK.SearchFilters{}

	testSelect(t, db, cnr, f1, object.AddressOf(o1))
	testSelect(t, db, cnr, f2, object.AddressOf(o2))
	testSelect(t, db, cnr, fAll, object.AddressOf(o1), object.AddressOf(o2))

	// Removed object

	ts1 := generateObject(t)
	require.NoError(t, metaInhume(db, object.AddressOf(o1), object.AddressOf(ts1)))

	oo, err := metaSelect(db, cnr, f1)
	require.NoError(t, err)
	require.Empty(t, oo)

	testSelect(t, db, cnr, fAll, object.AddressOf(o2))

	// Expired (== removed) but locked

	l := generateObject(t)
	require.NoError(t, db.Lock(cnr, object.AddressOf(l).Object(), []oid.ID{object.AddressOf(o3).Object()}))

	testSelect(t, db, cnr, fAll, object.AddressOf(o2), object.AddressOf(o3))
}

func benchmarkSelect(b *testing.B, db *meta.DB, cid cidSDK.ID, fs objectSDK.SearchFilters, expected int) {
	var prm meta.SelectPrm
	prm.SetContainerID(cid)
	prm.SetFilters(fs)

	for i := 0; i < b.N; i++ {
		res, err := db.Select(prm)
		if err != nil {
			b.Fatal(err)
		}
		if len(res.AddressList()) != expected {
			b.Fatalf("expected %d items, got %d", expected, len(res.AddressList()))
		}
	}
}

func metaSelect(db *meta.DB, cnr cidSDK.ID, fs objectSDK.SearchFilters) ([]oid.Address, error) {
	var prm meta.SelectPrm
	prm.SetFilters(fs)
	prm.SetContainerID(cnr)

	res, err := db.Select(prm)
	return res.AddressList(), err
}
