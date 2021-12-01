package meta_test

import (
	"encoding/hex"
	"testing"

	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

func TestDB_SelectUserAttributes(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	raw1 := generateRawObjectWithCID(t, cid)
	addAttribute(raw1, "foo", "bar")
	addAttribute(raw1, "x", "y")

	err := putBig(db, raw1.Object())
	require.NoError(t, err)

	raw2 := generateRawObjectWithCID(t, cid)
	addAttribute(raw2, "foo", "bar")
	addAttribute(raw2, "x", "z")

	err = putBig(db, raw2.Object())
	require.NoError(t, err)

	raw3 := generateRawObjectWithCID(t, cid)
	addAttribute(raw3, "a", "b")

	err = putBig(db, raw3.Object())
	require.NoError(t, err)

	raw4 := generateRawObjectWithCID(t, cid)
	addAttribute(raw4, "path", "test/1/2")

	err = putBig(db, raw4.Object())
	require.NoError(t, err)

	raw5 := generateRawObjectWithCID(t, cid)
	addAttribute(raw5, "path", "test/1/3")

	err = putBig(db, raw5.Object())
	require.NoError(t, err)

	raw6 := generateRawObjectWithCID(t, cid)
	addAttribute(raw6, "path", "test/2/3")

	err = putBig(db, raw6.Object())
	require.NoError(t, err)

	fs := objectSDK.SearchFilters{}
	fs.AddFilter("foo", "bar", objectSDK.MatchStringEqual)
	testSelect(t, db, cid, fs,
		raw1.Object().Address(),
		raw2.Object().Address(),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("x", "y", objectSDK.MatchStringEqual)
	testSelect(t, db, cid, fs, raw1.Object().Address())

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("x", "y", objectSDK.MatchStringNotEqual)
	testSelect(t, db, cid, fs, raw2.Object().Address())

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("a", "b", objectSDK.MatchStringEqual)
	testSelect(t, db, cid, fs, raw3.Object().Address())

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("c", "d", objectSDK.MatchStringEqual)
	testSelect(t, db, cid, fs)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("foo", "", objectSDK.MatchNotPresent)
	testSelect(t, db, cid, fs,
		raw3.Object().Address(),
		raw4.Object().Address(),
		raw5.Object().Address(),
		raw6.Object().Address(),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("a", "", objectSDK.MatchNotPresent)
	testSelect(t, db, cid, fs,
		raw1.Object().Address(),
		raw2.Object().Address(),
		raw4.Object().Address(),
		raw5.Object().Address(),
		raw6.Object().Address(),
	)

	fs = objectSDK.SearchFilters{}
	testSelect(t, db, cid, fs,
		raw1.Object().Address(),
		raw2.Object().Address(),
		raw3.Object().Address(),
		raw4.Object().Address(),
		raw5.Object().Address(),
		raw6.Object().Address(),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("key", "", objectSDK.MatchNotPresent)
	testSelect(t, db, cid, fs,
		raw1.Object().Address(),
		raw2.Object().Address(),
		raw3.Object().Address(),
		raw4.Object().Address(),
		raw5.Object().Address(),
		raw6.Object().Address(),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("path", "test", objectSDK.MatchCommonPrefix)
	testSelect(t, db, cid, fs,
		raw4.Object().Address(),
		raw5.Object().Address(),
		raw6.Object().Address(),
	)

	fs = objectSDK.SearchFilters{}
	fs.AddFilter("path", "test/1", objectSDK.MatchCommonPrefix)
	testSelect(t, db, cid, fs,
		raw4.Object().Address(),
		raw5.Object().Address(),
	)
}

func TestDB_SelectRootPhyParent(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	// prepare

	small := generateRawObjectWithCID(t, cid)
	err := putBig(db, small.Object())
	require.NoError(t, err)

	ts := generateRawObjectWithCID(t, cid)
	ts.SetType(objectSDK.TypeTombstone)
	err = putBig(db, ts.Object())
	require.NoError(t, err)

	sg := generateRawObjectWithCID(t, cid)
	sg.SetType(objectSDK.TypeStorageGroup)
	err = putBig(db, sg.Object())
	require.NoError(t, err)

	leftChild := generateRawObjectWithCID(t, cid)
	leftChild.InitRelations()
	err = putBig(db, leftChild.Object())
	require.NoError(t, err)

	parent := generateRawObjectWithCID(t, cid)

	rightChild := generateRawObjectWithCID(t, cid)
	rightChild.SetParent(parent.Object().SDK())
	rightChild.SetParentID(parent.ID())
	err = putBig(db, rightChild.Object())
	require.NoError(t, err)

	link := generateRawObjectWithCID(t, cid)
	link.SetParent(parent.Object().SDK())
	link.SetParentID(parent.ID())
	link.SetChildren(leftChild.ID(), rightChild.ID())

	err = putBig(db, link.Object())
	require.NoError(t, err)

	t.Run("root objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddRootFilter()
		testSelect(t, db, cid, fs,
			small.Object().Address(),
			parent.Object().Address(),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterPropertyRoot, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("phy objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddPhyFilter()
		testSelect(t, db, cid, fs,
			small.Object().Address(),
			ts.Object().Address(),
			sg.Object().Address(),
			leftChild.Object().Address(),
			rightChild.Object().Address(),
			link.Object().Address(),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterPropertyPhy, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("regular objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, v2object.TypeRegular.String(), objectSDK.MatchStringEqual)
		testSelect(t, db, cid, fs,
			small.Object().Address(),
			leftChild.Object().Address(),
			rightChild.Object().Address(),
			link.Object().Address(),
			parent.Object().Address(),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, v2object.TypeRegular.String(), objectSDK.MatchStringNotEqual)
		testSelect(t, db, cid, fs,
			ts.Object().Address(),
			sg.Object().Address(),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("tombstone objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, v2object.TypeTombstone.String(), objectSDK.MatchStringEqual)
		testSelect(t, db, cid, fs, ts.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, v2object.TypeTombstone.String(), objectSDK.MatchStringNotEqual)
		testSelect(t, db, cid, fs,
			small.Object().Address(),
			leftChild.Object().Address(),
			rightChild.Object().Address(),
			link.Object().Address(),
			parent.Object().Address(),
			sg.Object().Address(),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("storage group objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, v2object.TypeStorageGroup.String(), objectSDK.MatchStringEqual)
		testSelect(t, db, cid, fs, sg.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderObjectType, v2object.TypeStorageGroup.String(), objectSDK.MatchStringNotEqual)
		testSelect(t, db, cid, fs,
			small.Object().Address(),
			leftChild.Object().Address(),
			rightChild.Object().Address(),
			link.Object().Address(),
			parent.Object().Address(),
			ts.Object().Address(),
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
			rightChild.Object().Address(),
			link.Object().Address(),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderParent, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("all objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		testSelect(t, db, cid, fs,
			small.Object().Address(),
			ts.Object().Address(),
			sg.Object().Address(),
			leftChild.Object().Address(),
			rightChild.Object().Address(),
			link.Object().Address(),
			parent.Object().Address(),
		)
	})
}

func TestDB_SelectInhume(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	raw1 := generateRawObjectWithCID(t, cid)
	err := putBig(db, raw1.Object())
	require.NoError(t, err)

	raw2 := generateRawObjectWithCID(t, cid)
	err = putBig(db, raw2.Object())
	require.NoError(t, err)

	fs := objectSDK.SearchFilters{}
	testSelect(t, db, cid, fs,
		raw1.Object().Address(),
		raw2.Object().Address(),
	)

	tombstone := objectSDK.NewAddress()
	tombstone.SetContainerID(cid)
	tombstone.SetObjectID(testOID())

	err = meta.Inhume(db, raw2.Object().Address(), tombstone)
	require.NoError(t, err)

	fs = objectSDK.SearchFilters{}
	testSelect(t, db, cid, fs,
		raw1.Object().Address(),
	)
}

func TestDB_SelectPayloadHash(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	raw1 := generateRawObjectWithCID(t, cid)
	err := putBig(db, raw1.Object())
	require.NoError(t, err)

	raw2 := generateRawObjectWithCID(t, cid)
	err = putBig(db, raw2.Object())
	require.NoError(t, err)

	fs := objectSDK.SearchFilters{}
	fs.AddFilter(v2object.FilterHeaderPayloadHash,
		hex.EncodeToString(raw1.PayloadChecksum().Sum()),
		objectSDK.MatchStringEqual)

	testSelect(t, db, cid, fs, raw1.Object().Address())

	fs = objectSDK.SearchFilters{}
	fs.AddFilter(v2object.FilterHeaderPayloadHash,
		hex.EncodeToString(raw1.PayloadChecksum().Sum()),
		objectSDK.MatchStringNotEqual)

	testSelect(t, db, cid, fs, raw2.Object().Address())

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

	raw1 := generateRawObjectWithCID(t, cid)
	raw1.SetPayloadSize(10)
	raw1.SetCreationEpoch(11)
	raw1.SetVersion(v20)
	err := putBig(db, raw1.Object())
	require.NoError(t, err)

	raw2 := generateRawObjectWithCID(t, cid)
	raw2.SetPayloadSize(20)
	raw2.SetCreationEpoch(21)
	raw2.SetVersion(v21)
	err = putBig(db, raw2.Object())
	require.NoError(t, err)

	t.Run("object with TZHash", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderHomomorphicHash,
			hex.EncodeToString(raw1.PayloadHomomorphicHash().Sum()),
			objectSDK.MatchStringEqual)

		testSelect(t, db, cid, fs, raw1.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderHomomorphicHash,
			hex.EncodeToString(raw1.PayloadHomomorphicHash().Sum()),
			objectSDK.MatchStringNotEqual)

		testSelect(t, db, cid, fs, raw2.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderHomomorphicHash,
			"",
			objectSDK.MatchNotPresent)

		testSelect(t, db, cid, fs)
	})

	t.Run("object with payload length", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderPayloadLength, "20", objectSDK.MatchStringEqual)

		testSelect(t, db, cid, fs, raw2.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderPayloadLength, "20", objectSDK.MatchStringNotEqual)

		testSelect(t, db, cid, fs, raw1.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderPayloadLength, "", objectSDK.MatchNotPresent)

		testSelect(t, db, cid, fs)
	})

	t.Run("object with creation epoch", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderCreationEpoch, "11", objectSDK.MatchStringEqual)

		testSelect(t, db, cid, fs, raw1.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderCreationEpoch, "11", objectSDK.MatchStringNotEqual)

		testSelect(t, db, cid, fs, raw2.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderCreationEpoch, "", objectSDK.MatchNotPresent)

		testSelect(t, db, cid, fs)
	})

	t.Run("object with version", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectVersionFilter(objectSDK.MatchStringEqual, v21)
		testSelect(t, db, cid, fs, raw2.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddObjectVersionFilter(objectSDK.MatchStringNotEqual, v21)
		testSelect(t, db, cid, fs, raw1.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddObjectVersionFilter(objectSDK.MatchNotPresent, nil)
		testSelect(t, db, cid, fs)
	})
}

func TestDB_SelectObjectID(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	// prepare

	parent := generateRawObjectWithCID(t, cid)

	regular := generateRawObjectWithCID(t, cid)
	regular.SetParentID(parent.ID())
	regular.SetParent(parent.Object().SDK())

	err := putBig(db, regular.Object())
	require.NoError(t, err)

	ts := generateRawObjectWithCID(t, cid)
	ts.SetType(objectSDK.TypeTombstone)
	err = putBig(db, ts.Object())
	require.NoError(t, err)

	sg := generateRawObjectWithCID(t, cid)
	sg.SetType(objectSDK.TypeStorageGroup)
	err = putBig(db, sg.Object())
	require.NoError(t, err)

	t.Run("not present", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchNotPresent, nil)
		testSelect(t, db, cid, fs)
	})

	t.Run("not found objects", func(t *testing.T) {
		raw := generateRawObjectWithCID(t, cid)

		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, raw.ID())

		testSelect(t, db, cid, fs)

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, raw.ID())

		testSelect(t, db, cid, fs,
			regular.Object().Address(),
			parent.Object().Address(),
			sg.Object().Address(),
			ts.Object().Address(),
		)
	})

	t.Run("regular objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, regular.ID())
		testSelect(t, db, cid, fs, regular.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, regular.ID())
		testSelect(t, db, cid, fs,
			parent.Object().Address(),
			sg.Object().Address(),
			ts.Object().Address(),
		)
	})

	t.Run("tombstone objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, ts.ID())
		testSelect(t, db, cid, fs, ts.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, ts.ID())
		testSelect(t, db, cid, fs,
			regular.Object().Address(),
			parent.Object().Address(),
			sg.Object().Address(),
		)
	})

	t.Run("storage group objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, sg.ID())
		testSelect(t, db, cid, fs, sg.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, sg.ID())
		testSelect(t, db, cid, fs,
			regular.Object().Address(),
			parent.Object().Address(),
			ts.Object().Address(),
		)
	})

	t.Run("parent objects", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, parent.ID())
		testSelect(t, db, cid, fs, parent.Object().Address())

		fs = objectSDK.SearchFilters{}
		fs.AddObjectIDFilter(objectSDK.MatchStringNotEqual, parent.ID())
		testSelect(t, db, cid, fs,
			regular.Object().Address(),
			sg.Object().Address(),
			ts.Object().Address(),
		)
	})
}

func TestDB_SelectSplitID(t *testing.T) {
	db := newDB(t)

	cid := cidtest.ID()

	child1 := generateRawObjectWithCID(t, cid)
	child2 := generateRawObjectWithCID(t, cid)
	child3 := generateRawObjectWithCID(t, cid)

	split1 := objectSDK.NewSplitID()
	split2 := objectSDK.NewSplitID()

	child1.SetSplitID(split1)
	child2.SetSplitID(split1)
	child3.SetSplitID(split2)

	require.NoError(t, putBig(db, child1.Object()))
	require.NoError(t, putBig(db, child2.Object()))
	require.NoError(t, putBig(db, child3.Object()))

	t.Run("not present", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderSplitID, "", objectSDK.MatchNotPresent)
		testSelect(t, db, cid, fs)
	})

	t.Run("split id", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderSplitID, split1.String(), objectSDK.MatchStringEqual)
		testSelect(t, db, cid, fs,
			child1.Object().Address(),
			child2.Object().Address(),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddFilter(v2object.FilterHeaderSplitID, split2.String(), objectSDK.MatchStringEqual)
		testSelect(t, db, cid, fs, child3.Object().Address())
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

	obj1 := generateRawObjectWithCID(t, cid)
	err := putBig(db, obj1.Object())
	require.NoError(t, err)

	obj2 := generateRawObjectWithCID(t, cid)
	err = putBig(db, obj2.Object())
	require.NoError(t, err)

	t.Run("same cid", func(t *testing.T) {
		fs := objectSDK.SearchFilters{}
		fs.AddObjectContainerIDFilter(objectSDK.MatchStringEqual, cid)

		testSelect(t, db, cid, fs,
			obj1.Object().Address(),
			obj2.Object().Address(),
		)

		fs = objectSDK.SearchFilters{}
		fs.AddObjectContainerIDFilter(objectSDK.MatchStringNotEqual, cid)

		testSelect(t, db, cid, fs,
			obj1.Object().Address(),
			obj2.Object().Address(),
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
