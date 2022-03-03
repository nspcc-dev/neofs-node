package meta_test

import (
	"encoding/hex"
	"testing"

	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

func TestDB_SelectUserAttributes(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	raw1 := generateObjectWithCID(t, cid)
	addAttribute(raw1, "foo", "bar")
	addAttribute(raw1, "x", "y")

	err := putBig(db, raw1)
	require.NoError(t, err)

	raw2 := generateObjectWithCID(t, cid)
	addAttribute(raw2, "foo", "bar")
	addAttribute(raw2, "x", "z")

	err = putBig(db, raw2)
	require.NoError(t, err)

	raw3 := generateObjectWithCID(t, cid)
	addAttribute(raw3, "a", "b")

	err = putBig(db, raw3)
	require.NoError(t, err)

	raw4 := generateObjectWithCID(t, cid)
	addAttribute(raw4, "path", "test/1/2")

	err = putBig(db, raw4)
	require.NoError(t, err)

	raw5 := generateObjectWithCID(t, cid)
	addAttribute(raw5, "path", "test/1/3")

	err = putBig(db, raw5)
	require.NoError(t, err)

	raw6 := generateObjectWithCID(t, cid)
	addAttribute(raw6, "path", "test/2/3")

	err = putBig(db, raw6)
	require.NoError(t, err)

	fs := objectSDK.SearchFilters{}
	fs.AddFilter("foo", "bar", objectSDK.MatchStringEqual)
	testSelect(t, db, cid, fs,
		object.AddressOf(raw1),
		object.AddressOf(raw2),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("x", "y", objectSDK.MatchStringEqual)
	testSelect(t, db, cid, fs, object.AddressOf(raw1))

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("x", "y", objectSDK.MatchStringNotEqual)
	testSelect(t, db, cid, fs, object.AddressOf(raw2))

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("a", "b", objectSDK.MatchStringEqual)
	testSelect(t, db, cid, fs, object.AddressOf(raw3))

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("c", "d", objectSDK.MatchStringEqual)
	testSelect(t, db, cid, fs)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("foo", "", objectSDK.MatchNotPresent)
	testSelect(t, db, cid, fs,
		object.AddressOf(raw3),
		object.AddressOf(raw4),
		object.AddressOf(raw5),
		object.AddressOf(raw6),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("a", "", objectSDK.MatchNotPresent)
	testSelect(t, db, cid, fs,
		object.AddressOf(raw1),
		object.AddressOf(raw2),
		object.AddressOf(raw4),
		object.AddressOf(raw5),
		object.AddressOf(raw6),
	)

	fs = objectSDK.SearchFilters{}
	testSelect(t, db, cid, fs,
		object.AddressOf(raw1),
		object.AddressOf(raw2),
		object.AddressOf(raw3),
		object.AddressOf(raw4),
		object.AddressOf(raw5),
		object.AddressOf(raw6),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("key", "", objectSDK.MatchNotPresent)
	testSelect(t, db, cid, fs,
		object.AddressOf(raw1),
		object.AddressOf(raw2),
		object.AddressOf(raw3),
		object.AddressOf(raw4),
		object.AddressOf(raw5),
		object.AddressOf(raw6),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("path", "test", objectSDK.MatchCommonPrefix)
	testSelect(t, db, cid, fs,
		object.AddressOf(raw4),
		object.AddressOf(raw5),
		object.AddressOf(raw6),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("path", "test/1", objectSDK.MatchCommonPrefix)
	testSelect(t, db, cid, fs,
		object.AddressOf(raw4),
		object.AddressOf(raw5),
	)
}

func TestDB_SelectRootPhyParent(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	// prepare

	small := generateObjectWithCID(t, cid)
	err := putBig(db, small)
	require.NoError(t, err)

	ts := generateObjectWithCID(t, cid)
	ts.SetType(objectSDK.TypeTombstone)
	err = putBig(db, ts)
	require.NoError(t, err)

	sg := generateObjectWithCID(t, cid)
	sg.SetType(objectSDK.TypeStorageGroup)
	err = putBig(db, sg)
	require.NoError(t, err)

	leftChild := generateObjectWithCID(t, cid)
	leftChild.InitRelations()
	err = putBig(db, leftChild)
	require.NoError(t, err)

	parent := generateObjectWithCID(t, cid)

	rightChild := generateObjectWithCID(t, cid)
	rightChild.SetParent(parent)
	rightChild.SetParentID(parent.ID())
	err = putBig(db, rightChild)
	require.NoError(t, err)

	link := generateObjectWithCID(t, cid)
	link.SetParent(parent)
	link.SetParentID(parent.ID())
	link.SetChildren(leftChild.ID(), rightChild.ID())

	err = putBig(db, link)
	require.NoError(t, err)

	t.Run("root objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddRootFilter()
		testSelect(t, db, cid, fs,
			object.AddressOf(small),
			object.AddressOf(parent),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterPropertyRoot, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("phy objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddPhyFilter()
		testSelect(t, db, cid, fs,
			object.AddressOf(small),
			object.AddressOf(ts),
			object.AddressOf(sg),
			object.AddressOf(leftChild),
			object.AddressOf(rightChild),
			object.AddressOf(link),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterPropertyPhy, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("regular objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, v2object.TypeRegular.String(), objectSDK.MatchStringEqual)
		testSelect(t, db, cid, fs,
			object.AddressOf(small),
			object.AddressOf(leftChild),
			object.AddressOf(rightChild),
			object.AddressOf(link),
			object.AddressOf(parent),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, v2object.TypeRegular.String(), objectSDK.MatchStringNotEqual)
		testSelect(t, db, cid, fs,
			object.AddressOf(ts),
			object.AddressOf(sg),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("tombstone objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, v2object.TypeTombstone.String(), objectSDK.MatchStringEqual)
		testSelect(t, db, cid, fs, object.AddressOf(ts))

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, v2object.TypeTombstone.String(), objectSDK.MatchStringNotEqual)
		testSelect(t, db, cid, fs,
			object.AddressOf(small),
			object.AddressOf(leftChild),
			object.AddressOf(rightChild),
			object.AddressOf(link),
			object.AddressOf(parent),
			object.AddressOf(sg),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("storage group objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, v2object.TypeStorageGroup.String(), objectSDK.MatchStringEqual)
		testSelect(t, db, cid, fs, object.AddressOf(sg))

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, v2object.TypeStorageGroup.String(), objectSDK.MatchStringNotEqual)
		testSelect(t, db, cid, fs,
			object.AddressOf(small),
			object.AddressOf(leftChild),
			object.AddressOf(rightChild),
			object.AddressOf(link),
			object.AddressOf(parent),
			object.AddressOf(ts),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("objects with parent", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderParent,
			parent.ID().String(),
			objectSDK.MatchStringEqual)

		testSelect(t, db, cid, fs,
			object.AddressOf(rightChild),
			object.AddressOf(link),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderParent, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("all objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		testSelect(t, db, cid, fs,
			object.AddressOf(small),
			object.AddressOf(ts),
			object.AddressOf(sg),
			object.AddressOf(leftChild),
			object.AddressOf(rightChild),
			object.AddressOf(link),
			object.AddressOf(parent),
		)
	})
}

func TestDB_SelectInhume(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	raw1 := generateObjectWithCID(t, cid)
	err := putBig(db, raw1)
	require.NoError(t, err)

	raw2 := generateObjectWithCID(t, cid)
	err = putBig(db, raw2)
	require.NoError(t, err)

	fs := objectSDK.SearchFilters{}
	testSelect(t, db, cid, fs,
		object.AddressOf(raw1),
		object.AddressOf(raw2),
	)

	tombstone := addressSDK.NewAddress()
	tombstone.SetContainerID(cid)
	tombstone.SetObjectID(testOID())

	err = meta.Inhume(db, object.AddressOf(raw2), tombstone)
	require.NoError(t, err)

	fs = objectSDK.SearchFilters{}
	testSelect(t, db, cid, fs,
		object.AddressOf(raw1),
	)
}

func TestDB_SelectPayloadHash(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	raw1 := generateObjectWithCID(t, cid)
	err := putBig(db, raw1)
	require.NoError(t, err)

	raw2 := generateObjectWithCID(t, cid)
	err = putBig(db, raw2)
	require.NoError(t, err)

	fs := objectSDK.SearchFilters{}
	fs.AddFilter(v2object.FilterHeaderPayloadHash,
		hex.EncodeToString(raw1.PayloadChecksum().Sum()),
		objectSDK.MatchStringEqual)

	testSelect(t, db, cid, fs, object.AddressOf(raw1))

	fs = objectSDK.SearchFilters{}
	fs.AddFilter(v2object.FilterHeaderPayloadHash,
		hex.EncodeToString(raw1.PayloadChecksum().Sum()),
		objectSDK.MatchStringNotEqual)

	testSelect(t, db, cid, fs, object.AddressOf(raw2))

	fs = objectSDK.SearchFilters{}
	fs.AddFilter(v2object.FilterHeaderPayloadHash,
		"",
		objectSDK.MatchNotPresent)

	testSelect(t, db, cid, fs)
}

func TestDB_SelectWithSlowFilters(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	v20 := new(version.Version)
	v20.SetMajor(2)

	v21 := new(version.Version)
	v21.SetMajor(2)
	v21.SetMinor(1)

	raw1 := generateObjectWithCID(t, cid)
	raw1.SetPayloadSize(10)
	raw1.SetCreationEpoch(11)
	raw1.SetVersion(v20)
	err := putBig(db, raw1)
	require.NoError(t, err)

	raw2 := generateObjectWithCID(t, cid)
	raw2.SetPayloadSize(20)
	raw2.SetCreationEpoch(21)
	raw2.SetVersion(v21)
	err = putBig(db, raw2)
	require.NoError(t, err)

	t.Run("object with TZHash", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderHomomorphicHash,
			hex.EncodeToString(raw1.PayloadHomomorphicHash().Sum()),
			objectSDK.MatchStringEqual)

		testSelect(t, db, cid, fs, object.AddressOf(raw1))

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderHomomorphicHash,
			hex.EncodeToString(raw1.PayloadHomomorphicHash().Sum()),
			objectSDK.MatchStringNotEqual)

		testSelect(t, db, cid, fs, object.AddressOf(raw2))

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderHomomorphicHash,
			"",
			objectSDK.MatchNotPresent)

		testSelect(t, db, cid, fs)
	})

	t.Run("object with payload length", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderPayloadLength, "20", objectSDK.MatchStringEqual)

		testSelect(t, db, cid, fs, object.AddressOf(raw2))

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderPayloadLength, "20", objectSDK.MatchStringNotEqual)

		testSelect(t, db, cid, fs, object.AddressOf(raw1))

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderPayloadLength, "", objectSDK.MatchNotPresent)

		testSelect(t, db, cid, fs)
	})

	t.Run("object with creation epoch", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderCreationEpoch, "11", objectSDK.MatchStringEqual)

		testSelect(t, db, cid, fs, object.AddressOf(raw1))

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderCreationEpoch, "11", objectSDK.MatchStringNotEqual)

		testSelect(t, db, cid, fs, object.AddressOf(raw2))

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderCreationEpoch, "", objectSDK.MatchNotPresent)

		testSelect(t, db, cid, fs)
	})

	t.Run("object with version", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectVersionFilter(objectSDK.MatchStringEqual, v21)
		testSelect(t, db, cid, fs, object.AddressOf(raw2))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectVersionFilter(objectSDK.MatchStringNotEqual, v21)
		testSelect(t, db, cid, fs, object.AddressOf(raw1))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectVersionFilter(objectSDK.MatchNotPresent, nil)
		testSelect(t, db, cid, fs)
	})
}

func TestDB_SelectObjectID(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	// prepare

	parent := generateObjectWithCID(t, cid)

	regular := generateObjectWithCID(t, cid)
	regular.SetParentID(parent.ID())
	regular.SetParent(parent)

	err := putBig(db, regular)
	require.NoError(t, err)

	ts := generateObjectWithCID(t, cid)
	ts.SetType(objectSDK.TypeTombstone)
	err = putBig(db, ts)
	require.NoError(t, err)

	sg := generateObjectWithCID(t, cid)
	sg.SetType(objectSDK.TypeStorageGroup)
	err = putBig(db, sg)
	require.NoError(t, err)

	t.Run("not present", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchNotPresent, nil)
		testSelect(t, db, cid, fs)
	})

	t.Run("not found objects", func(t *testing.T) {
		raw := generateObjectWithCID(t, cid)

		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, raw.ID())

		testSelect(t, db, cid, fs)

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, raw.ID())

		testSelect(t, db, cid, fs,
			object.AddressOf(regular),
			object.AddressOf(parent),
			object.AddressOf(sg),
			object.AddressOf(ts),
		)
	})

	t.Run("regular objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, regular.ID())
		testSelect(t, db, cid, fs, object.AddressOf(regular))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, regular.ID())
		testSelect(t, db, cid, fs,
			object.AddressOf(parent),
			object.AddressOf(sg),
			object.AddressOf(ts),
		)
	})

	t.Run("tombstone objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, ts.ID())
		testSelect(t, db, cid, fs, object.AddressOf(ts))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, ts.ID())
		testSelect(t, db, cid, fs,
			object.AddressOf(regular),
			object.AddressOf(parent),
			object.AddressOf(sg),
		)
	})

	t.Run("storage group objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, sg.ID())
		testSelect(t, db, cid, fs, object.AddressOf(sg))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, sg.ID())
		testSelect(t, db, cid, fs,
			object.AddressOf(regular),
			object.AddressOf(parent),
			object.AddressOf(ts),
		)
	})

	t.Run("parent objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, parent.ID())
		testSelect(t, db, cid, fs, object.AddressOf(parent))

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, parent.ID())
		testSelect(t, db, cid, fs,
			object.AddressOf(regular),
			object.AddressOf(sg),
			object.AddressOf(ts),
		)
	})
}

func TestDB_SelectSplitID(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	child1 := generateObjectWithCID(t, cid)
	child2 := generateObjectWithCID(t, cid)
	child3 := generateObjectWithCID(t, cid)

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
		fs.AddFilter(v2object.FilterHeaderSplitID, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("split id", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderSplitID, split1.String(), objectSDK.MatchStringEqual)
		testSelect(t, db, cid, fs,
			object.AddressOf(child1),
			object.AddressOf(child2),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderSplitID, split2.String(), objectSDK.MatchStringEqual)
		testSelect(t, db, cid, fs, object.AddressOf(child3))
	})

	t.Run("empty split", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderSplitID, "", objectSDK.MatchStringEqual)
		testSelect(t, db, cid, fs)
	})

	t.Run("unknown split id", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderSplitID,
			objectSDK.NewSplitID().String(),
			objectSDK.MatchStringEqual)
		testSelect(t, db, cid, fs)
	})
}

func TestDB_SelectContainerID(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	obj1 := generateObjectWithCID(t, cid)
	err := putBig(db, obj1)
	require.NoError(t, err)

	obj2 := generateObjectWithCID(t, cid)
	err = putBig(db, obj2)
	require.NoError(t, err)

	t.Run("same cid", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectContainerIDFilter(objectSDK.MatchStringEqual, cid)

		testSelect(t, db, cid, fs,
			object.AddressOf(obj1),
			object.AddressOf(obj2),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddObjectContainerIDFilter(objectSDK.MatchStringNotEqual, cid)

		testSelect(t, db, cid, fs,
			object.AddressOf(obj1),
			object.AddressOf(obj2),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddObjectContainerIDFilter(objectSDK.MatchNotPresent, cid)

		testSelect(t, db, cid, fs)
	})

	t.Run("not same cid", func(t *testing.T) {
		newCID := cidtest.ID()

		fs := objectSDK.SearchFilters{}
		fs.AddObjectContainerIDFilter(objectSDK.MatchStringEqual, newCID)

		testSelect(t, db, cid, fs)
	})
}
