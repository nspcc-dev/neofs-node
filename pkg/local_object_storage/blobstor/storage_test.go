package blobstor_test

import (
	"crypto/rand"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestCopy(t *testing.T) {
	testCopy(t, blobstor.Copy)
}

func TestCopyBatched(t *testing.T) {
	testCopy(t, func(dst, src blobstor.Storage) error {
		return blobstor.CopyBatched(dst, src, 7)
	})
}

func TestPayloadRangeResolve(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rng        blobstor.PayloadRange
		payloadLen uint64
		off        uint64
		ln         uint64
		outOfRange bool
	}{
		{name: "unset", payloadLen: 10, ln: 10},
		{name: "empty payload", rng: blobstor.NewPayloadRange(0, 0)},
		{name: "full fixed", rng: blobstor.NewPayloadRange(0, 0), payloadLen: 10, ln: 10},
		{name: "fixed", rng: blobstor.NewPayloadRange(2, 3), payloadLen: 10, off: 2, ln: 3},
		{name: "invalid fixed", rng: blobstor.NewPayloadRange(2, 0), payloadLen: 10, outOfRange: true},
		{name: "fixed out of range", rng: blobstor.NewPayloadRange(8, 3), payloadLen: 10, outOfRange: true},
		{name: "bounds", rng: blobstor.NewPayloadRangeBounds(2, 5), payloadLen: 10, off: 2, ln: 4},
		{name: "bounds clipped", rng: blobstor.NewPayloadRangeBounds(2, 20), payloadLen: 10, off: 2, ln: 8},
		{name: "reversed bounds", rng: blobstor.NewPayloadRangeBounds(5, 2), payloadLen: 10, outOfRange: true},
		{name: "bounds out of range", rng: blobstor.NewPayloadRangeBounds(10, 20), payloadLen: 10, outOfRange: true},
		{name: "from", rng: blobstor.NewPayloadRangeFrom(2), payloadLen: 10, off: 2, ln: 8},
		{name: "from out of range", rng: blobstor.NewPayloadRangeFrom(10), payloadLen: 10, outOfRange: true},
		{name: "suffix", rng: blobstor.NewPayloadRangeSuffix(3), payloadLen: 10, off: 7, ln: 3},
		{name: "suffix clipped", rng: blobstor.NewPayloadRangeSuffix(20), payloadLen: 10, ln: 10},
		{name: "empty payload suffix", rng: blobstor.NewPayloadRangeSuffix(3)},
		{name: "zero suffix", rng: blobstor.NewPayloadRangeSuffix(0), payloadLen: 10, outOfRange: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			off, ln, err := tc.rng.Resolve(tc.payloadLen)
			switch {
			case tc.outOfRange:
				require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange)
			default:
				require.NoError(t, err)
				require.Equal(t, tc.off, off)
				require.Equal(t, tc.ln, ln)
			}
		})
	}
}

func testCopy(t *testing.T, copier func(dst, src blobstor.Storage) error) {
	dir := t.TempDir()
	const nObjects = 100

	src := fstree.New(fstree.WithPath(filepath.Join(dir, "src")))

	require.NoError(t, src.Open(false))
	require.NoError(t, src.Init(blobstor.ID{}))

	mObjs := make(map[oid.Address][]byte, nObjects)

	for range nObjects {
		addr := oidtest.Address()
		data := make([]byte, 32)
		_, _ = rand.Read(data)
		mObjs[addr] = data

		err := src.Put(addr, data)
		require.NoError(t, err)
	}

	require.NoError(t, src.Close())

	dst := fstree.New(fstree.WithPath(filepath.Join(dir, "dst")))

	err := copier(dst, src)
	require.NoError(t, err)

	require.NoError(t, dst.Open(true))
	t.Cleanup(func() { _ = dst.Close() })

	dstObjs := make(map[oid.Address][]byte, nObjects)

	err = dst.Iterate(func(addr oid.Address, data []byte) error {
		dstObjs[addr] = data
		return nil
	}, nil)
	require.Equal(t, mObjs, dstObjs)
	require.NoError(t, err)
}
