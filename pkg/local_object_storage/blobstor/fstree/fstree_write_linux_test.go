//go:build linux

package fstree

import (
	"encoding/binary"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

func TestFSTree_InitPut(t *testing.T) {
	t.Run("combined", func(t *testing.T) {
		const dataLen = 100 << 10
		data := testutil.RandByteSlice(dataLen)

		fst := setupFSTree(t)

		assertInitPut(t, fst, testAddress, data)

		expData := calculateObjectDataInCombinedFile(testObjectID, data)

		testutil.AssertSingleDirFileData(t, filepath.Join(fst.RootPath, testObjectDir), testObjectFileName, expData)

		t.Run("already exists", func(t *testing.T) {
			assertInitPut(t, fst, testAddress, data)

			testutil.AssertSingleDirFileData(t, filepath.Join(fst.RootPath, testObjectDir), testObjectFileName, expData)
		})
	})

	testInitPutGeneric(t)
}

func calculateObjectDataInCombinedFile(id oid.ID, data []byte) []byte {
	dataLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(dataLenBytes, uint32(len(data)))
	return slices.Concat([]byte{127, 0}, id[:], dataLenBytes, data)
}
