package fstree

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

var (
	testContainerID = cid.ID{155, 205, 32, 87, 141, 19, 108, 127, 18, 253, 6, 184, 28, 192, 108, 227, 82, 141, 155, 95, 0, 80, 38, 158, 97, 139, 237, 33, 44, 2, 160, 29}
	testObjectID    = oid.ID{195, 62, 227, 206, 13, 60, 235, 227, 21, 40, 86, 14, 111, 228, 7, 43, 110, 203, 223, 147, 41, 184, 41, 85, 84, 31, 168, 145, 195, 78, 118, 36}
	testAddress     = oid.NewAddress(testContainerID, testObjectID)
	// corresponds to testAddress and depth=4.
	testObjectDir = filepath.Join("E", "9", "A", "8")
)

const (
	// corresponds to testAddress and depth=4.
	testObjectFileName = "2e5awsXzDz4FHEmoz81sVU4c3EQzd1oVM122RVST.BVBcG4LStyX486XkjmwcXytTsiEsed2tPkxEP8USaV4g"
)

func BenchmarkFSTree_InitPut(b *testing.B) {
	for _, payloadLen := range []uint64{
		1,
		4 << 10,
		4 << 20,
		64 << 20,
	} {
		b.Run(fmt.Sprintf("len=%d", payloadLen), func(b *testing.B) {
			fst := setupFSTree(b, disableCombinedWriteOpt)

			header := testutil.RandByteSlice(1 << 10)
			payload := testutil.RandByteSlice(payloadLen)

			const maxChunkLength = 256 << 10

			chunks := slices.Collect(slices.Chunk(payload, maxChunkLength))

			for b.Loop() {
				addr := oidtest.Address()
				assertInitPut(b, fst, addr, header, chunks...)
			}
		})
	}
}

func testInitPutGenericWithLength(t *testing.T, payloadLen uint64) {
	const headerLen = 1 << 10
	header := testutil.RandByteSlice(headerLen)
	payload := testutil.RandByteSlice(payloadLen)

	t.Run("abort", func(t *testing.T) {
		t.Run("before stream", func(t *testing.T) {
			fst := setupFSTree(t, disableCombinedWriteOpt)

			stream, abortFn, err := fst.InitPut(testAddress, headerLen, payloadLen, bytes.NewBuffer(header))
			require.NoError(t, err)

			abortFn()

			testutil.AssertEmptyDir(t, filepath.Join(fst.RootPath, testObjectDir))

			assertWriteStreamAlreadyAborted(t, stream)
		})
		t.Run("after write before close", func(t *testing.T) {
			fst := setupFSTree(t, disableCombinedWriteOpt)

			stream, abortFn, err := fst.InitPut(testAddress, headerLen, payloadLen, bytes.NewBuffer(header))
			require.NoError(t, err)

			n, err := stream.Write(payload)
			require.NoError(t, err)
			require.EqualValues(t, len(payload), n)

			abortFn()

			testutil.AssertEmptyDir(t, filepath.Join(fst.RootPath, testObjectDir))

			assertWriteStreamAlreadyAborted(t, stream)
		})
		t.Run("after stream", func(t *testing.T) {
			fst := setupFSTree(t, disableCombinedWriteOpt)

			stream, abortFn := assertInitPut(t, fst, testAddress, header, payload)

			expData := concatHeaderAndPayload(header, payload)

			testutil.AssertSingleDirFileData(t, filepath.Join(fst.RootPath, testObjectDir), testObjectFileName, expData)

			abortFn()

			testutil.AssertSingleDirFileData(t, filepath.Join(fst.RootPath, testObjectDir), testObjectFileName, expData)

			assertWriteStreamAlreadyAborted(t, stream)
		})
	})

	t.Run("multi-write", func(t *testing.T) {
		fst := setupFSTree(t, disableCombinedWriteOpt)

		chunks := [][]byte{
			testutil.RandByteSlice(32),
			testutil.RandByteSlice(payloadLen),
			testutil.RandByteSlice(123),
			testutil.RandByteSlice(2 * payloadLen),
			testutil.RandByteSlice(1),
		}

		assertInitPut(t, fst, testAddress, header, chunks...)

		expData := concatHeaderAndPayload(header, slices.Concat(chunks...))

		testutil.AssertSingleDirFileData(t, filepath.Join(fst.RootPath, testObjectDir), testObjectFileName, expData)
	})

	fst := setupFSTree(t, disableCombinedWriteOpt)

	assertInitPut(t, fst, testAddress, payload)

	testutil.AssertSingleDirFileData(t, filepath.Join(fst.RootPath, testObjectDir), testObjectFileName, payload)

	t.Run("already exists", func(t *testing.T) {
		assertInitPut(t, fst, testAddress, payload)

		testutil.AssertSingleDirFileData(t, filepath.Join(fst.RootPath, testObjectDir), testObjectFileName, payload)
	})
}

func testInitPutGeneric(t *testing.T) {
	for _, dataLen := range []uint64{
		1,
		4 << 10,
		256 << 10,
		1 << 20,
		4 << 20,
		64 << 20,
	} {
		t.Run(fmt.Sprintf("len=%d", dataLen), func(t *testing.T) {
			testInitPutGenericWithLength(t, dataLen)
		})
	}
}

func assertWriteStreamAlreadyAborted(t *testing.T, stream io.WriteCloser) {
	_, err := stream.Write([]byte{0})
	require.EqualError(t, err, "stream already aborted")
	err = stream.Close()
	require.EqualError(t, err, "stream already aborted")
}
