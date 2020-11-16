package meta

import (
	"crypto/rand"
	"crypto/sha256"
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func testSelect(t *testing.T, db *DB, fs objectSDK.SearchFilters, exp ...*objectSDK.Address) {
	res, err := db.Select(fs)
	require.NoError(t, err)
	require.Len(t, res, len(exp))

	for i := range exp {
		require.Contains(t, res, exp[i])
	}
}

func testCID() *container.ID {
	cs := [sha256.Size]byte{}
	rand.Read(cs[:])

	id := container.NewID()
	id.SetSHA256(cs)

	return id
}

func testOID() *objectSDK.ID {
	cs := [sha256.Size]byte{}
	rand.Read(cs[:])

	id := objectSDK.NewID()
	id.SetSHA256(cs)

	return id
}

func TestDB(t *testing.T) {
	version := pkg.NewVersion()
	version.SetMajor(2)
	version.SetMinor(1)

	cid := testCID()

	w, err := owner.NEO3WalletFromPublicKey(&test.DecodeKey(-1).PublicKey)
	require.NoError(t, err)

	ownerID := owner.NewID()
	ownerID.SetNeo3Wallet(w)

	oid := testOID()

	obj := object.NewRaw()
	obj.SetID(oid)
	obj.SetOwnerID(ownerID)
	obj.SetContainerID(cid)
	obj.SetVersion(version)

	k, v := "key", "value"

	a := objectSDK.NewAttribute()
	a.SetKey(k)
	a.SetValue(v)

	obj.SetAttributes(a)

	db := newDB(t)

	defer releaseDB(db)

	o := obj.Object()

	require.NoError(t, db.Put(o))

	o2, err := db.Get(o.Address())
	require.NoError(t, err)

	require.Equal(t, o, o2)

	fs := objectSDK.SearchFilters{}

	// filter container ID
	fs.AddObjectContainerIDFilter(objectSDK.MatchStringEqual, cid)
	testSelect(t, db, fs, o.Address())

	// filter owner ID
	fs.AddObjectOwnerIDFilter(objectSDK.MatchStringEqual, ownerID)
	testSelect(t, db, fs, o.Address())

	// filter attribute
	fs.AddFilter(k, v, objectSDK.MatchStringEqual)
	testSelect(t, db, fs, o.Address())

	// filter mismatch
	fs.AddFilter(k, v+"1", objectSDK.MatchStringEqual)
	testSelect(t, db, fs)
}

func TestDB_Delete(t *testing.T) {
	db := newDB(t)

	defer releaseDB(db)

	obj := object.NewRaw()
	obj.SetContainerID(testCID())
	obj.SetID(testOID())

	o := obj.Object()

	require.NoError(t, db.Put(o))

	addr := o.Address()

	_, err := db.Get(addr)
	require.NoError(t, err)

	fs := objectSDK.SearchFilters{}
	fs.AddObjectContainerIDFilter(objectSDK.MatchStringEqual, o.ContainerID())

	testSelect(t, db, fs, o.Address())

	require.NoError(t, db.Delete(addr))

	_, err = db.Get(addr)
	require.Error(t, err)

	testSelect(t, db, fs)
}

func TestDB_SelectProperties(t *testing.T) {
	db := newDB(t)

	defer releaseDB(db)

	parent := object.NewRaw()
	parent.SetContainerID(testCID())
	parent.SetID(testOID())

	child := object.NewRaw()
	child.SetContainerID(testCID())
	child.SetID(testOID())
	child.SetParent(parent.Object().SDK())

	parAddr := parent.Object().Address()
	childAddr := child.Object().Address()

	require.NoError(t, db.Put(child.Object()))

	// root filter
	fs := objectSDK.SearchFilters{}
	fs.AddRootFilter()
	testSelect(t, db, fs, parAddr)

	// phy filter
	fs = fs[:0]
	fs.AddPhyFilter()
	testSelect(t, db, fs, childAddr)

	lnk := object.NewRaw()
	lnk.SetContainerID(testCID())
	lnk.SetID(testOID())
	lnk.SetChildren(testOID())

	lnkAddr := lnk.Object().Address()

	require.NoError(t, db.Put(lnk.Object()))

	// childfree filter
	fs = fs[:0]
	fs.AddChildfreeFilter()
	testSelect(t, db, fs, childAddr, parAddr)

	// non-childfree filter
	fs = fs[:0]
	fs.AddNonChildfreeFilter()
	testSelect(t, db, fs, lnkAddr)

	// childfree filter (with random false value)
	fs = fs[:0]
	fs.AddFilter(v2object.FilterPropertyChildfree, "some false value", objectSDK.MatchStringEqual)
	testSelect(t, db, fs, lnkAddr)
}

func TestDB_Path(t *testing.T) {
	path := t.Name()

	bdb, err := bbolt.Open(path, 0600, nil)
	require.NoError(t, err)

	db := NewDB(FromBoltDB(bdb))

	defer releaseDB(db)

	require.Equal(t, path, db.Path())
}

func newDB(t testing.TB) *DB {
	path := t.Name()

	bdb, err := bbolt.Open(path, 0600, nil)
	require.NoError(t, err)

	return NewDB(FromBoltDB(bdb))
}

func releaseDB(db *DB) {
	db.Close()
	os.Remove(db.Path())
}

func TestSelectNonExistentAttributes(t *testing.T) {
	db := newDB(t)

	defer releaseDB(db)

	obj := object.NewRaw()
	obj.SetID(testOID())
	obj.SetContainerID(testCID())

	require.NoError(t, db.Put(obj.Object()))

	fs := objectSDK.SearchFilters{}

	// add filter by non-existent attribute
	fs.AddFilter("key", "value", objectSDK.MatchStringEqual)

	res, err := db.Select(fs)
	require.NoError(t, err)
	require.Empty(t, res)
}

func TestVirtualObject(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	// create object with parent
	obj := generateObject(t, testPrm{
		withParent: true,
	})

	require.NoError(t, db.Put(obj))

	childAddr := obj.Address()
	parAddr := obj.GetParent().Address()

	// child object must be readable
	_, err := db.Get(childAddr)
	require.NoError(t, err)

	// parent object must not be readable
	_, err = db.Get(parAddr)
	require.True(t, errors.Is(err, errNotFound))

	fs := objectSDK.SearchFilters{}

	// both objects should appear in selection
	testSelect(t, db, fs, childAddr, parAddr)

	// filter leaves
	fs.AddPhyFilter()

	// only child object should appear
	testSelect(t, db, fs, childAddr)

	fs = fs[:0]

	// filter non-leaf objects
	fs.AddRootFilter()

	// only parent object should appear
	testSelect(t, db, fs, parAddr)
}
