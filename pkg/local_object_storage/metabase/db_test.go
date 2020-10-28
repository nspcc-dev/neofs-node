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
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
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

func TestDB(t *testing.T) {
	version := pkg.NewVersion()
	version.SetMajor(2)
	version.SetMinor(1)

	cs := [sha256.Size]byte{}
	rand.Read(cs[:])

	cid := container.NewID()
	cid.SetSHA256(cs)

	w, err := owner.NEO3WalletFromPublicKey(&test.DecodeKey(-1).PublicKey)
	require.NoError(t, err)

	ownerID := owner.NewID()
	ownerID.SetNeo3Wallet(w)

	rand.Read(cs[:])
	oid := objectSDK.NewID()
	oid.SetSHA256(cs)

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

	path := "test.db"

	bdb, err := bbolt.Open(path, 0600, nil)
	require.NoError(t, err)

	defer func() {
		bdb.Close()
		os.Remove(path)
	}()

	db := NewDB(bdb)

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
