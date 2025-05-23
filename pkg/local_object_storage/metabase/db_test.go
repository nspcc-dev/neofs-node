package meta_test

import (
	"crypto/rand"
	"os"
	"strconv"
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

type epochState struct{ e uint64 }

func (s epochState) CurrentEpoch() uint64 {
	if s.e != 0 {
		return s.e
	}

	return 0
}

// saves "big" object in DB.
func putBig(db *meta.DB, obj *object.Object) error {
	return metaPut(db, obj)
}

func testSelect(t *testing.T, db *meta.DB, cnr cid.ID, fs object.SearchFilters, exp ...oid.Address) {
	res, err := metaSelect(db, cnr, fs)
	require.NoError(t, err)
	require.Len(t, res, len(exp))

	for i := range exp {
		require.Contains(t, res, exp[i])
	}
}

func newDB(t testing.TB, opts ...meta.Option) *meta.DB {
	path := t.Name()

	bdb := meta.New(
		append([]meta.Option{
			meta.WithPath(path),
			meta.WithPermissions(0o600),
			meta.WithEpochState(epochState{}),
		}, opts...)...,
	)

	require.NoError(t, bdb.Open(false))
	require.NoError(t, bdb.Init())

	t.Cleanup(func() {
		bdb.Close()
		os.Remove(bdb.DumpInfo().Path)
	})

	return bdb
}

func generateObject(t testing.TB) *object.Object {
	return generateObjectWithCID(t, cidtest.ID())
}

func generateObjectWithCID(t testing.TB, cnr cid.ID) *object.Object {
	var ver version.Version
	ver.SetMajor(2)
	ver.SetMinor(1)

	payload := make([]byte, 10)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	csum, err := checksum.NewFromData(checksum.SHA256, payload)
	require.NoError(t, err)
	csumTZ, err := checksum.NewFromData(checksum.TillichZemor, payload)
	require.NoError(t, err)

	obj := object.New()
	obj.SetID(oidtest.ID())
	owner := usertest.ID()
	obj.SetOwner(owner)
	obj.SetContainerID(cnr)
	obj.SetVersion(&ver)
	obj.SetPayloadChecksum(csum)
	obj.SetPayloadHomomorphicHash(csumTZ)
	obj.SetPayload(payload)
	obj.SetPayloadSize(uint64(len(payload)))

	return obj
}

func addAttribute(obj *object.Object, key, val string) {
	var attr object.Attribute
	attr.SetKey(key)
	attr.SetValue(val)

	attrs := obj.Attributes()
	attrs = append(attrs, attr)
	obj.SetAttributes(attrs...)
}

func checkExpiredObjects(t *testing.T, db *meta.DB, f func(exp, nonExp *object.Object)) {
	expObj := generateObject(t)
	setExpiration(expObj, currEpoch-1)

	require.NoError(t, metaPut(db, expObj))

	nonExpObj := generateObject(t)
	setExpiration(nonExpObj, currEpoch)

	require.NoError(t, metaPut(db, nonExpObj))

	f(expObj, nonExpObj)
}

func setExpiration(o *object.Object, epoch uint64) {
	var attr object.Attribute

	attr.SetKey(object.AttributeExpirationEpoch)
	attr.SetValue(strconv.FormatUint(epoch, 10))

	o.SetAttributes(append(o.Attributes(), attr)...)
}
