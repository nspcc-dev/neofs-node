package meta_test

import (
	"crypto/sha256"
	"math/rand"
	"os"
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	ownertest "github.com/nspcc-dev/neofs-sdk-go/owner/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
)

// saves "big" object in DB.
func putBig(db *meta.DB, obj *object.Object) error {
	return meta.Put(db, obj, nil)
}

func testSelect(t *testing.T, db *meta.DB, cid *cid.ID, fs object.SearchFilters, exp ...*addressSDK.Address) {
	res, err := meta.Select(db, cid, fs)
	require.NoError(t, err)
	require.Len(t, res, len(exp))

	for i := range exp {
		require.Contains(t, res, exp[i])
	}
}

func testOID() *oidSDK.ID {
	cs := [sha256.Size]byte{}
	_, _ = rand.Read(cs[:])

	id := oidSDK.NewID()
	id.SetSHA256(cs)

	return id
}

func newDB(t testing.TB) *meta.DB {
	path := t.Name()

	bdb := meta.New(meta.WithPath(path), meta.WithPermissions(0600))

	require.NoError(t, bdb.Open())

	t.Cleanup(func() {
		bdb.Close()
		os.Remove(bdb.DumpInfo().Path)
	})

	return bdb
}

func generateObject(t *testing.T) *object.Object {
	return generateObjectWithCID(t, cidtest.ID())
}

func generateObjectWithCID(t *testing.T, cid *cid.ID) *object.Object {
	version := version.New()
	version.SetMajor(2)
	version.SetMinor(1)

	csum := new(checksum.Checksum)
	csum.SetSHA256(sha256.Sum256(owner.PublicKeyToIDBytes(&test.DecodeKey(-1).PublicKey)))

	csumTZ := new(checksum.Checksum)
	csumTZ.SetTillichZemor(tz.Sum(csum.Sum()))

	obj := object.New()
	obj.SetID(testOID())
	obj.SetOwnerID(ownertest.ID())
	obj.SetContainerID(cid)
	obj.SetVersion(version)
	obj.SetPayloadChecksum(csum)
	obj.SetPayloadHomomorphicHash(csumTZ)
	obj.SetPayload([]byte{1, 2, 3, 4, 5})

	return obj
}

func generateAddress() *addressSDK.Address {
	addr := addressSDK.NewAddress()
	addr.SetContainerID(cidtest.ID())
	addr.SetObjectID(testOID())

	return addr
}

func addAttribute(obj *object.Object, key, val string) {
	var attr object.Attribute
	attr.SetKey(key)
	attr.SetValue(val)

	attrs := obj.Attributes()
	attrs = append(attrs, attr)
	obj.SetAttributes(attrs...)
}
