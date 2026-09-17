//go:build !linux

package fstree

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/internal/testutil/fstest"
	"github.com/stretchr/testify/require"
)

func TestFSTree_InitPut(t *testing.T) {
	const headerLen = 1 << 10
	header := testutil.RandByteSlice(headerLen)
	const payloadLen = 256 << 10
	payload := testutil.RandByteSlice(payloadLen)

	fullObject := separateHeaderAndPayload(header, payload)

	t.Run("retries", func(t *testing.T) {
		t.Run("existing tmp files", func(t *testing.T) {
			const maxRetries = 5
			for i := range maxRetries {
				retryNum := i + 1
				t.Run(strconv.Itoa(retryNum), func(t *testing.T) {
					fst := setupFSTree(t)

					for i := range retryNum {
						require.NoError(t, os.MkdirAll(filepath.Join(fst.RootPath, testObjectDir), fst.Permissions))
						fstest.TouchFile(t, filepath.Join(fst.RootPath, testObjectDir, testObjectFileName+"#"+strconv.Itoa(i)))
					}

					stream, _, err := fst.InitPut(testAddress, headerLen, payloadLen, bytes.NewBuffer(header))
					if retryNum < maxRetries {
						require.NoError(t, err)

						n, err := stream.Write(payload)
						require.NoError(t, err)
						require.EqualValues(t, len(payload), n)

						require.NoError(t, stream.Close())

						fstest.AssertFileData(t, filepath.Join(fst.RootPath, testObjectDir, testObjectFileName), fullObject)
					} else {
						require.ErrorIs(t, err, fs.ErrExist)
						fstest.AssertFileNotExists(t, filepath.Join(fst.RootPath, testObjectDir, testObjectFileName))
					}

					for i := range retryNum {
						fstest.AssertFileExists(t, filepath.Join(fst.RootPath, testObjectDir, testObjectFileName+"#"+strconv.Itoa(i)))
					}
					for i := retryNum; i < maxRetries; i++ {
						fstest.AssertFileNotExists(t, filepath.Join(fst.RootPath, testObjectDir, testObjectFileName+"#"+strconv.Itoa(i)))
					}
				})
			}
		})
	})

	testInitPutGeneric(t)
}
