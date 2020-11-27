package meta_test

import (
	"encoding/hex"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/stretchr/testify/require"
)

func TestDB_SelectUserAttributes(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	cid := testCID()

	raw1 := generateRawObjectWithCID(t, cid)
	addAttribute(raw1, "foo", "bar")
	addAttribute(raw1, "x", "y")

	err := db.Put(raw1.Object(), nil)
	require.NoError(t, err)

	raw2 := generateRawObjectWithCID(t, cid)
	addAttribute(raw2, "foo", "bar")
	addAttribute(raw2, "x", "z")

	err = db.Put(raw2.Object(), nil)
	require.NoError(t, err)

	raw3 := generateRawObjectWithCID(t, cid)
	addAttribute(raw3, "a", "b")

	err = db.Put(raw3.Object(), nil)
	require.NoError(t, err)

	fs := generateSearchFilter(cid)
	fs.AddFilter("foo", "bar", objectSDK.MatchStringEqual)
	testSelect(t, db, fs,
		raw1.Object().Address(),
		raw2.Object().Address(),
	)

	fs = generateSearchFilter(cid)
	fs.AddFilter("x", "y", objectSDK.MatchStringEqual)
	testSelect(t, db, fs, raw1.Object().Address())

	fs = generateSearchFilter(cid)
	fs.AddFilter("a", "b", objectSDK.MatchStringEqual)
	testSelect(t, db, fs, raw3.Object().Address())

	fs = generateSearchFilter(cid)
	fs.AddFilter("c", "d", objectSDK.MatchStringEqual)
	testSelect(t, db, fs)

	fs = generateSearchFilter(cid)
	testSelect(t, db, fs,
		raw1.Object().Address(),
		raw2.Object().Address(),
		raw3.Object().Address(),
	)
}

func TestDB_SelectRootPhyParent(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	cid := testCID()

	// prepare

	small := generateRawObjectWithCID(t, cid)
	err := db.Put(small.Object(), nil)
	require.NoError(t, err)

	ts := generateRawObjectWithCID(t, cid)
	ts.SetType(objectSDK.TypeTombstone)
	err = db.Put(ts.Object(), nil)
	require.NoError(t, err)

	sg := generateRawObjectWithCID(t, cid)
	sg.SetType(objectSDK.TypeStorageGroup)
	err = db.Put(sg.Object(), nil)
	require.NoError(t, err)

	leftChild := generateRawObjectWithCID(t, cid)
	leftChild.InitRelations()
	err = db.Put(leftChild.Object(), nil)
	require.NoError(t, err)

	parent := generateRawObjectWithCID(t, cid)

	rightChild := generateRawObjectWithCID(t, cid)
	rightChild.SetParent(parent.Object().SDK())
	rightChild.SetParentID(parent.ID())
	err = db.Put(rightChild.Object(), nil)
	require.NoError(t, err)

	link := generateRawObjectWithCID(t, cid)
	link.SetParent(parent.Object().SDK())
	link.SetParentID(parent.ID())
	link.SetChildren(leftChild.ID(), rightChild.ID())

	err = db.Put(link.Object(), nil)
	require.NoError(t, err)

	t.Run("root objects", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddRootFilter()
		testSelect(t, db, fs,
			small.Object().Address(),
			parent.Object().Address(),
		)
	})

	t.Run("phy objects", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddPhyFilter()
		testSelect(t, db, fs,
			small.Object().Address(),
			ts.Object().Address(),
			sg.Object().Address(),
			leftChild.Object().Address(),
			rightChild.Object().Address(),
			link.Object().Address(),
		)
	})

	t.Run("regular objects", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddFilter(v2object.FilterHeaderObjectType, "Regular", objectSDK.MatchStringEqual)
		testSelect(t, db, fs,
			small.Object().Address(),
			leftChild.Object().Address(),
			rightChild.Object().Address(),
			link.Object().Address(),
			parent.Object().Address(),
		)
	})

	t.Run("tombstone objects", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddFilter(v2object.FilterHeaderObjectType, "Tombstone", objectSDK.MatchStringEqual)
		testSelect(t, db, fs, ts.Object().Address())
	})

	t.Run("storage group objects", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddFilter(v2object.FilterHeaderObjectType, "StorageGroup", objectSDK.MatchStringEqual)
		testSelect(t, db, fs, sg.Object().Address())
	})

	t.Run("objects with parent", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddFilter(v2object.FilterHeaderParent,
			parent.ID().String(),
			objectSDK.MatchStringEqual)

		testSelect(t, db, fs,
			rightChild.Object().Address(),
			link.Object().Address(),
		)
	})

	t.Run("all objects", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		testSelect(t, db, fs,
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
	defer releaseDB(db)

	cid := testCID()

	raw1 := generateRawObjectWithCID(t, cid)
	err := db.Put(raw1.Object(), nil)
	require.NoError(t, err)

	raw2 := generateRawObjectWithCID(t, cid)
	err = db.Put(raw2.Object(), nil)
	require.NoError(t, err)

	fs := generateSearchFilter(cid)
	testSelect(t, db, fs,
		raw1.Object().Address(),
		raw2.Object().Address(),
	)

	tombstone := objectSDK.NewAddress()
	tombstone.SetContainerID(cid)
	tombstone.SetObjectID(testOID())

	err = db.Inhume(raw2.Object().Address(), tombstone)
	require.NoError(t, err)

	fs = generateSearchFilter(cid)
	testSelect(t, db, fs,
		raw1.Object().Address(),
	)
}

func TestDB_SelectPayloadHash(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	cid := testCID()

	raw1 := generateRawObjectWithCID(t, cid)
	err := db.Put(raw1.Object(), nil)
	require.NoError(t, err)

	raw2 := generateRawObjectWithCID(t, cid)
	err = db.Put(raw2.Object(), nil)
	require.NoError(t, err)

	fs := generateSearchFilter(cid)
	fs.AddFilter(v2object.FilterHeaderPayloadHash,
		hex.EncodeToString(raw1.PayloadChecksum().Sum()),
		objectSDK.MatchStringEqual)

	testSelect(t, db, fs, raw1.Object().Address())
}

func TestDB_SelectWithSlowFilters(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	cid := testCID()

	v20 := new(pkg.Version)
	v20.SetMajor(2)

	v21 := new(pkg.Version)
	v21.SetMajor(2)
	v21.SetMinor(1)

	raw1 := generateRawObjectWithCID(t, cid)
	raw1.SetPayloadSize(10)
	raw1.SetCreationEpoch(11)
	raw1.SetVersion(v20)
	err := db.Put(raw1.Object(), nil)
	require.NoError(t, err)

	raw2 := generateRawObjectWithCID(t, cid)
	raw2.SetPayloadSize(20)
	raw2.SetCreationEpoch(21)
	raw2.SetVersion(v21)
	err = db.Put(raw2.Object(), nil)
	require.NoError(t, err)

	t.Run("object with TZHash", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddFilter(v2object.FilterHeaderHomomorphicHash,
			hex.EncodeToString(raw1.PayloadHomomorphicHash().Sum()),
			objectSDK.MatchStringEqual)

		testSelect(t, db, fs, raw1.Object().Address())
	})

	t.Run("object with payload length", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddFilter(v2object.FilterHeaderPayloadLength, "20", objectSDK.MatchStringEqual)

		testSelect(t, db, fs, raw2.Object().Address())
	})

	t.Run("object with creation epoch", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddFilter(v2object.FilterHeaderCreationEpoch, "11", objectSDK.MatchStringEqual)

		testSelect(t, db, fs, raw1.Object().Address())
	})

	t.Run("object with version", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddObjectVersionFilter(objectSDK.MatchStringEqual, v21)
		testSelect(t, db, fs, raw2.Object().Address())
	})
}

func generateSearchFilter(cid *container.ID) objectSDK.SearchFilters {
	fs := objectSDK.SearchFilters{}
	fs.AddObjectContainerIDFilter(objectSDK.MatchStringEqual, cid)

	return fs
}

func TestDB_SelectObjectID(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	cid := testCID()

	// prepare

	parent := generateRawObjectWithCID(t, cid)

	regular := generateRawObjectWithCID(t, cid)
	regular.SetParentID(parent.ID())
	regular.SetParent(parent.Object().SDK())

	err := db.Put(regular.Object(), nil)
	require.NoError(t, err)

	ts := generateRawObjectWithCID(t, cid)
	ts.SetType(objectSDK.TypeTombstone)
	err = db.Put(ts.Object(), nil)
	require.NoError(t, err)

	sg := generateRawObjectWithCID(t, cid)
	sg.SetType(objectSDK.TypeStorageGroup)
	err = db.Put(sg.Object(), nil)
	require.NoError(t, err)

	t.Run("not found objects", func(t *testing.T) {
		raw := generateRawObjectWithCID(t, cid)

		fs := generateSearchFilter(cid)
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, raw.ID())

		testSelect(t, db, fs)
	})

	t.Run("regular objects", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, regular.ID())
		testSelect(t, db, fs, regular.Object().Address())
	})

	t.Run("tombstone objects", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, ts.ID())
		testSelect(t, db, fs, ts.Object().Address())
	})

	t.Run("storage group objects", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, sg.ID())
		testSelect(t, db, fs, sg.Object().Address())
	})

	t.Run("storage group objects", func(t *testing.T) {
		fs := generateSearchFilter(cid)
		fs.AddObjectIDFilter(objectSDK.MatchStringEqual, parent.ID())
		testSelect(t, db, fs, parent.Object().Address())
	})
}
