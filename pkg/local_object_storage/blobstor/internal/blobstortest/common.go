package blobstortest

import (
	"math/rand"
	"testing"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

// Constructor constructs blobstor component.
// Each call must create a component using different file-system path.
type Constructor = func(t *testing.T) common.Storage

// objectDesc is a helper structure to avoid multiple `Marshal` invokes during tests.
type objectDesc struct {
	obj       *objectSDK.Object
	addr      oid.Address
	raw       []byte
	storageID []byte
}

func TestAll(t *testing.T, cons Constructor, min, max uint64) {
	t.Run("get", func(t *testing.T) {
		TestGet(t, cons, min, max)
	})
	t.Run("get range", func(t *testing.T) {
		TestGetRange(t, cons, min, max)
	})
	t.Run("delete", func(t *testing.T) {
		TestDelete(t, cons, min, max)
	})
	t.Run("exists", func(t *testing.T) {
		TestExists(t, cons, min, max)
	})
	t.Run("iterate", func(t *testing.T) {
		TestIterate(t, cons, min, max)
	})
}

func TestInfo(t *testing.T, cons Constructor, expectedType string, expectedPath string) {
	s := cons(t)
	require.Equal(t, expectedType, s.Type())
	require.Equal(t, expectedPath, s.Path())
}

func prepare(t *testing.T, count int, s common.Storage, min, max uint64) []objectDesc {
	objects := make([]objectDesc, count)

	for i := range objects {
		objects[i].obj = NewObject(min + uint64(rand.Intn(int(max-min+1)))) // not too large
		objects[i].addr = objectCore.AddressOf(objects[i].obj)

		raw, err := objects[i].obj.Marshal()
		require.NoError(t, err)
		objects[i].raw = raw
	}

	for i := range objects {
		var prm common.PutPrm
		prm.Address = objects[i].addr
		prm.Object = objects[i].obj
		prm.RawData = objects[i].raw

		putRes, err := s.Put(prm)
		require.NoError(t, err)

		objects[i].storageID = putRes.StorageID
	}

	return objects
}

// NewObject creates a regular object of specified size with a random payload.
func NewObject(sz uint64) *objectSDK.Object {
	raw := objectSDK.New()

	raw.SetID(oidtest.ID())
	raw.SetContainerID(cidtest.ID())

	payload := make([]byte, sz)
	//nolint:staticcheck
	rand.Read(payload)
	raw.SetPayload(payload)

	// fit the binary size to the required
	data, _ := raw.Marshal()
	if ln := uint64(len(data)); ln > sz {
		raw.SetPayload(raw.Payload()[:sz-(ln-sz)])
	}

	return raw
}
