package meta_test

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"math/big"
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

	firstChild := oidtest.ID()

	leftChild := generateObjectWithCID(t, cnr)
	leftChild.SetFirstID(firstChild)
	err = putBig(db, leftChild)
	require.NoError(t, err)

	lock := generateObjectWithCID(t, cnr)
	lock.SetType(objectSDK.TypeLock)
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
		idParent := parent.GetID()

		fs := objectSDK.SearchFilters{}
		fs.AddParentIDFilter(objectSDK.MatchStringEqual, idParent)

		testSelect(t, db, cnr, fs,
			object.AddressOf(rightChild),
			object.AddressOf(link),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(objectSDK.FilterParentID, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cnr, fs)

		fs = objectSDK.SearchFilters{}
		fs.AddFirstSplitObjectFilter(objectSDK.MatchStringEqual, firstChild)

		testSelect(t, db, cnr, fs,
			object.AddressOf(leftChild),
			object.AddressOf(rightChild),
			object.AddressOf(link),
		)
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
	payloadHash := [sha256.Size]byte(cs.Value())

	t.Run("equal filter", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddPayloadHashFilter(objectSDK.MatchStringEqual, payloadHash)

		testSelect(t, db, cnr, fs, object.AddressOf(raw1))
	})

	t.Run("common prefix filter", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(objectSDK.FilterPayloadChecksum,
			hex.EncodeToString(payloadHash[:len(payloadHash)-1]),
			objectSDK.MatchCommonPrefix)

		testSelect(t, db, cnr, fs, object.AddressOf(raw1))
	})

	t.Run("not equal filter", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddPayloadHashFilter(objectSDK.MatchStringNotEqual, payloadHash)

		testSelect(t, db, cnr, fs, object.AddressOf(raw2))
	})

	t.Run("not present filter", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(objectSDK.FilterPayloadChecksum,
			"",
			objectSDK.MatchNotPresent)

		testSelect(t, db, cnr, fs)
	})

	t.Run("invalid hashes", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
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
		homoHash := [tz.Size]byte(cs.Value())

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
	idParent := parent.GetID()
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

		id := raw.GetID()

		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, id)

		testSelect(t, db, cnr, fs)

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, id)

		_, err = metaGet(db, object.AddressOf(regular), false)
		require.NoError(t, err)
		_, err = metaGet(db, object.AddressOf(parent), false)
		require.NoError(t, err)
		_, err = metaGet(db, object.AddressOf(sg), false)
		require.NoError(t, err)
		_, err = metaGet(db, object.AddressOf(ts), false)
		require.NoError(t, err)
		_, err = metaGet(db, object.AddressOf(lock), false)
		require.NoError(t, err)

		testSelect(t, db, cnr, fs,
			object.AddressOf(regular),
			object.AddressOf(parent),
			object.AddressOf(sg),
			object.AddressOf(ts),
			object.AddressOf(lock),
		)
	})

	t.Run("regular objects", func(t *testing.T) {
		id := regular.GetID()

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
		id := ts.GetID()

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
		id := sg.GetID()

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
		id := parent.GetID()

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
		id := lock.GetID()

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

	for i := range objCount {
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
		fs.AddFilter("myHeader", strconv.Itoa(objCount/2), objectSDK.MatchUnspecified)
		benchmarkSelect(b, db, cid, fs, 0)
	})
}

func TestExpiredObjects(t *testing.T) {
	db := newDB(t, meta.WithEpochState(epochState{currEpoch}))

	checkExpiredObjects(t, db, func(exp, nonExp *objectSDK.Object) {
		cidExp := exp.GetContainerID()
		cidNonExp := nonExp.GetContainerID()

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
	for range b.N {
		addrs, err := db.Select(cid, fs)
		if err != nil {
			b.Fatal(err)
		}
		if len(addrs) != expected {
			b.Fatalf("expected %d items, got %d", expected, len(addrs))
		}
	}
}

func metaSelect(db *meta.DB, cnr cidSDK.ID, fs objectSDK.SearchFilters) ([]oid.Address, error) {
	return db.Select(cnr, fs)
}

func numQuery(key string, op objectSDK.SearchMatchType, val string) (res objectSDK.SearchFilters) {
	res.AddFilter(key, val, op)
	return
}

var allNumOps = []objectSDK.SearchMatchType{
	objectSDK.MatchNumGT,
	objectSDK.MatchNumGE,
	objectSDK.MatchNumLT,
	objectSDK.MatchNumLE,
}

func TestNumericSelect(t *testing.T) {
	db := newDB(t)
	cnr := cidtest.ID()

	for i, testAttr := range []struct {
		key string
		set func(obj *objectSDK.Object, val uint64)
	}{
		{key: "$Object:creationEpoch", set: func(obj *objectSDK.Object, val uint64) { obj.SetCreationEpoch(val) }},
		{key: "$Object:payloadLength", set: func(obj *objectSDK.Object, val uint64) { obj.SetPayloadSize(val) }},
		{key: "any_user_attr", set: func(obj *objectSDK.Object, val uint64) {
			addAttribute(obj, "any_user_attr", strconv.FormatUint(val, 10))
		}},
	} {
		cnr := cidtest.ID()
		obj1 := generateObjectWithCID(t, cnr)
		addr1 := object.AddressOf(obj1)
		obj2 := generateObjectWithCID(t, cnr)
		addr2 := object.AddressOf(obj2)

		const smallNum = 10
		const midNum = 11
		const bigNum = 12

		testAttr.set(obj1, smallNum)
		testAttr.set(obj2, bigNum)

		require.NoError(t, putBig(db, obj1), i)
		require.NoError(t, putBig(db, obj2), i)

		for j, testCase := range []struct {
			op  objectSDK.SearchMatchType
			num uint64
			exp []oid.Address
		}{
			{op: objectSDK.MatchNumLT, num: smallNum - 1, exp: nil},
			{op: objectSDK.MatchNumLT, num: smallNum, exp: nil},
			{op: objectSDK.MatchNumLT, num: midNum, exp: []oid.Address{addr1}},
			{op: objectSDK.MatchNumLT, num: bigNum, exp: []oid.Address{addr1}},
			{op: objectSDK.MatchNumLT, num: bigNum + 1, exp: []oid.Address{addr1, addr2}},

			{op: objectSDK.MatchNumLE, num: smallNum - 1, exp: nil},
			{op: objectSDK.MatchNumLE, num: smallNum, exp: []oid.Address{addr1}},
			{op: objectSDK.MatchNumLE, num: midNum, exp: []oid.Address{addr1}},
			{op: objectSDK.MatchNumLE, num: bigNum, exp: []oid.Address{addr1, addr2}},
			{op: objectSDK.MatchNumLE, num: bigNum + 1, exp: []oid.Address{addr1, addr2}},

			{op: objectSDK.MatchNumGE, num: smallNum - 1, exp: []oid.Address{addr1, addr2}},
			{op: objectSDK.MatchNumGE, num: smallNum, exp: []oid.Address{addr1, addr2}},
			{op: objectSDK.MatchNumGE, num: midNum, exp: []oid.Address{addr2}},
			{op: objectSDK.MatchNumGE, num: bigNum, exp: []oid.Address{addr2}},
			{op: objectSDK.MatchNumGE, num: bigNum + 1, exp: nil},

			{op: objectSDK.MatchNumGT, num: smallNum - 1, exp: []oid.Address{addr1, addr2}},
			{op: objectSDK.MatchNumGT, num: smallNum, exp: []oid.Address{addr2}},
			{op: objectSDK.MatchNumGT, num: midNum, exp: []oid.Address{addr2}},
			{op: objectSDK.MatchNumGT, num: bigNum, exp: nil},
			{op: objectSDK.MatchNumGT, num: bigNum + 1, exp: nil},
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
	addr := object.AddressOf(obj)

	require.NoError(t, putBig(db, obj))

	val := objVal - 1
	testSelect(t, db, cnrNeg, numQuery(negAttr, objectSDK.MatchNumGT, strconv.FormatInt(val, 10)), addr)
	testSelect(t, db, cnrNeg, numQuery(negAttr, objectSDK.MatchNumGE, strconv.FormatInt(val, 10)), addr)
	testSelect(t, db, cnrNeg, numQuery(negAttr, objectSDK.MatchNumLT, strconv.FormatInt(val, 10)))
	testSelect(t, db, cnrNeg, numQuery(negAttr, objectSDK.MatchNumLE, strconv.FormatInt(val, 10)))
	val = objVal
	testSelect(t, db, cnrNeg, numQuery(negAttr, objectSDK.MatchNumGT, strconv.FormatInt(val, 10)))
	testSelect(t, db, cnrNeg, numQuery(negAttr, objectSDK.MatchNumGE, strconv.FormatInt(val, 10)), addr)
	testSelect(t, db, cnrNeg, numQuery(negAttr, objectSDK.MatchNumLT, strconv.FormatInt(val, 10)))
	testSelect(t, db, cnrNeg, numQuery(negAttr, objectSDK.MatchNumLE, strconv.FormatInt(val, 10)), addr)
	val = objVal + 1
	testSelect(t, db, cnrNeg, numQuery(negAttr, objectSDK.MatchNumGT, strconv.FormatInt(val, 10)))
	testSelect(t, db, cnrNeg, numQuery(negAttr, objectSDK.MatchNumGE, strconv.FormatInt(val, 10)))
	testSelect(t, db, cnrNeg, numQuery(negAttr, objectSDK.MatchNumLT, strconv.FormatInt(val, 10)), addr)
	testSelect(t, db, cnrNeg, numQuery(negAttr, objectSDK.MatchNumLE, strconv.FormatInt(val, 10)), addr)

	// uint64 overflow
	for _, attr := range []string{
		"$Object:creationEpoch",
		"$Object:payloadLength",
	} {
		b := new(big.Int).SetUint64(math.MaxUint64)
		b.Add(b, big.NewInt(1))
		filterVal := b.String()

		testSelect(t, db, cnrNeg, numQuery(attr, objectSDK.MatchNumGT, filterVal))
		testSelect(t, db, cnrNeg, numQuery(attr, objectSDK.MatchNumGE, filterVal))
		testSelect(t, db, cnrNeg, numQuery(attr, objectSDK.MatchNumLT, filterVal), addr)
		testSelect(t, db, cnrNeg, numQuery(attr, objectSDK.MatchNumLE, filterVal), addr)
	}

	t.Run("invalid filtering value format", func(t *testing.T) {
		for _, op := range allNumOps {
			query := numQuery("any_key", op, "1.0")

			_, err := metaSelect(db, cnr, query)
			require.ErrorIs(t, err, object.ErrInvalidSearchQuery, op)
			require.ErrorContains(t, err, "numeric filter with non-decimal value", op)
		}
	})

	t.Run("non-numeric system attributes", func(t *testing.T) {
		for _, attr := range []string{
			"$Object:version",
			"$Object:objectID",
			"$Object:containerID",
			"$Object:ownerID",
			"$Object:payloadHash",
			"$Object:objectType",
			"$Object:homomorphicHash",
			"$Object:split.parent",
			"$Object:split.splitID",
			"$Object:ROOT",
			"$Object:PHY",
		} {
			for _, op := range allNumOps {
				query := numQuery(attr, op, "123")

				_, err := metaSelect(db, cnr, query)
				require.ErrorIs(t, err, object.ErrInvalidSearchQuery, [2]any{attr, op})
				require.ErrorContains(t, err, "numeric filter with non-numeric system object attribute", [2]any{attr, op})
			}
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
		addr1 := object.AddressOf(obj1)
		obj2 := generateObjectWithCID(t, cnr)
		addr2 := object.AddressOf(obj2)

		const attr = "any_user_attribute"
		addAttribute(obj1, attr, "123")
		addAttribute(obj2, attr, "any_non_num")

		require.NoError(t, putBig(db, obj1))
		require.NoError(t, putBig(db, obj2))

		var query objectSDK.SearchFilters

		testSelect(t, db, cnr, query,
			addr1, addr2)

		for _, testCase := range []struct {
			op  objectSDK.SearchMatchType
			val string
		}{
			{op: objectSDK.MatchNumGT, val: "122"},
			{op: objectSDK.MatchNumGE, val: "122"},
			{op: objectSDK.MatchNumGE, val: "123"},
			{op: objectSDK.MatchNumLE, val: "123"},
			{op: objectSDK.MatchNumLE, val: "124"},
			{op: objectSDK.MatchNumLT, val: "124"},
		} {
			query = numQuery(attr, testCase.op, testCase.val)

			res, err := metaSelect(db, cnr, query)
			require.NoError(t, err, testCase)
			require.Equal(t, []oid.Address{addr1}, res, testCase)
		}
	})
}

func TestSelectNotFilledBucket(t *testing.T) {
	db := newDB(t)
	cnr := cidtest.ID()

	raw1 := generateObjectWithCID(t, cnr)
	raw1.SetType(objectSDK.TypeRegular)
	addAttribute(raw1, "attr", "value")
	oID1 := raw1.GetID()

	raw2 := generateObjectWithCID(t, cnr)
	raw2.SetType(objectSDK.TypeTombstone)
	addAttribute(raw1, "attr", "value1")

	err := putBig(db, raw1)
	require.NoError(t, err)

	err = putBig(db, raw2)
	require.NoError(t, err)

	// it should find 2nd object despite the missing object buckets;
	// see the commit's fix
	fs := objectSDK.SearchFilters{}
	fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, oID1)
	testSelect(t, db, cnr, fs, object.AddressOf(raw2))
}
