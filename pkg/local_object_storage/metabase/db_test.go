package meta_test

import (
	"crypto/sha256"
	"math/rand"
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
)

// saves "big" object in DB.
func putBig(db *meta.DB, obj *object.Object) error {
	return meta.Put(db, obj, nil)
}

func testSelect(t *testing.T, db *meta.DB, cid *cid.ID, fs objectSDK.SearchFilters, exp ...*objectSDK.Address) {
	res, err := meta.Select(db, cid, fs)
	require.NoError(t, err)
	require.Len(t, res, len(exp))

	for i := range exp {
		require.Contains(t, res, exp[i])
	}
}

func testCID() *cid.ID {
	cs := [sha256.Size]byte{}
	_, _ = rand.Read(cs[:])

	id := cid.New()
	id.SetSHA256(cs)

	return id
}

func testOID() *objectSDK.ID {
	cs := [sha256.Size]byte{}
	_, _ = rand.Read(cs[:])

	id := objectSDK.NewID()
	id.SetSHA256(cs)

	return id
}

func newDB(t testing.TB) *meta.DB {
	path := t.Name()

	bdb := meta.New(meta.WithPath(path), meta.WithPermissions(0600))

	require.NoError(t, bdb.Open())

	return bdb
}

func releaseDB(db *meta.DB) {
	db.Close()
	os.Remove(db.DumpInfo().Path)
}

func generateRawObject(t *testing.T) *object.RawObject {
	return generateRawObjectWithCID(t, testCID())
}

func generateRawObjectWithCID(t *testing.T, cid *cid.ID) *object.RawObject {
	version := pkg.NewVersion()
	version.SetMajor(2)
	version.SetMinor(1)

	w, err := owner.NEO3WalletFromPublicKey(&test.DecodeKey(-1).PublicKey)
	require.NoError(t, err)

	ownerID := owner.NewID()
	ownerID.SetNeo3Wallet(w)

	csum := new(pkg.Checksum)
	csum.SetSHA256(sha256.Sum256(w.Bytes()))

	csumTZ := new(pkg.Checksum)
	csumTZ.SetTillichZemor(tz.Sum(csum.Sum()))

	obj := object.NewRaw()
	obj.SetID(testOID())
	obj.SetOwnerID(ownerID)
	obj.SetContainerID(cid)
	obj.SetVersion(version)
	obj.SetPayloadChecksum(csum)
	obj.SetPayloadHomomorphicHash(csumTZ)
	obj.SetPayload([]byte{1, 2, 3, 4, 5})

	return obj
}

func generateAddress() *objectSDK.Address {
	addr := objectSDK.NewAddress()
	addr.SetContainerID(testCID())
	addr.SetObjectID(testOID())

	return addr
}

func addAttribute(obj *object.RawObject, key, val string) {
	attr := objectSDK.NewAttribute()
	attr.SetKey(key)
	attr.SetValue(val)

	attrs := obj.Attributes()
	attrs = append(attrs, attr)
	obj.SetAttributes(attrs...)
}
