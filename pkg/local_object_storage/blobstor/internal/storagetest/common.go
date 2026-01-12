package storagetest

import (
	crand "crypto/rand"
	"math/rand/v2"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

// Constructor constructs storage component.
// Each call must create a component using different file-system path.
type Constructor = func(t *testing.T) common.Storage

// objectDesc is a helper structure to avoid multiple `Marshal` invokes during tests.
type objectDesc struct {
	obj  *object.Object
	addr oid.Address
	raw  []byte
}

func TestAll(t *testing.T, cons Constructor, minSize, maxSize uint64) {
	t.Run("get", func(t *testing.T) {
		TestGet(t, cons, minSize, maxSize)
	})
	t.Run("get range", func(t *testing.T) {
		TestGetRange(t, cons, minSize, maxSize)
	})
	t.Run("delete", func(t *testing.T) {
		TestDelete(t, cons, minSize, maxSize)
	})
	t.Run("exists", func(t *testing.T) {
		TestExists(t, cons, minSize, maxSize)
	})
	t.Run("iterate", func(t *testing.T) {
		TestIterate(t, cons, minSize, maxSize)
	})
}

func TestInfo(t *testing.T, cons Constructor, expectedType string, expectedPath string) {
	s := cons(t)
	require.Equal(t, expectedType, s.Type())
	require.Equal(t, expectedPath, s.Path())
}

func prepare(t *testing.T, count int, s common.Storage, minSize, maxSize uint64) []objectDesc {
	objects := make([]objectDesc, count)

	for i := range objects {
		objects[i].obj = NewObject(minSize + uint64(rand.IntN(int(maxSize-minSize+1)))) // not too large
		objects[i].addr = objectcore.AddressOf(objects[i].obj)
		objects[i].raw = objects[i].obj.Marshal()
	}

	for i := range objects {
		err := s.Put(objects[i].addr, objects[i].raw)
		require.NoError(t, err)
	}

	return objects
}

func prepareBatch(t *testing.T, count int, s common.Storage, minSize, maxSize uint64) []objectDesc {
	objects := make([]objectDesc, count)
	mObj := make(map[oid.Address][]byte, len(objects))

	for i := range objects {
		objects[i].obj = NewObject(minSize + uint64(rand.IntN(int(maxSize-minSize+1)))) // not too large
		objects[i].addr = objectcore.AddressOf(objects[i].obj)
		objects[i].raw = objects[i].obj.Marshal()

		mObj[objects[i].addr] = objects[i].raw
	}

	err := s.PutBatch(mObj)
	require.NoError(t, err)

	return objects
}

// NewObject creates a regular object of specified size with a random payload.
func NewObject(sz uint64) *object.Object {
	raw := object.New(cidtest.ID(), usertest.ID())

	raw.SetID(oidtest.ID())

	payload := make([]byte, sz)
	_, _ = crand.Read(payload)
	raw.SetPayload(payload)

	// fit the binary size to the required
	data := raw.Marshal()
	if ln := uint64(len(data)); ln > sz {
		raw.SetPayload(raw.Payload()[:sz-(ln-sz)])
	}
	raw.SetPayloadSize(uint64(len(raw.Payload())))

	return raw
}
