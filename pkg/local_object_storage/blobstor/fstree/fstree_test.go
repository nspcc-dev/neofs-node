package fstree

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestAddressToString(t *testing.T) {
	addr := oidtest.Address()
	s := stringifyAddress(addr)
	actual, err := addressFromString(s)
	require.NoError(t, err)
	require.Equal(t, addr, *actual)
}

func TestFSTree(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "neofs.fstree.test")
	require.NoError(t, os.Mkdir(tmpDir, os.ModePerm))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(tmpDir)) })

	fs := FSTree{
		Info: Info{
			Permissions: os.ModePerm,
			RootPath:    tmpDir,
		},
		Depth:      2,
		DirNameLen: 2,
	}

	const count = 3
	var addrs []oid.Address

	store := map[string][]byte{}

	for i := 0; i < count; i++ {
		a := oidtest.Address()
		addrs = append(addrs, a)

		data, err := objecttest.Object().Marshal()
		require.NoError(t, err)

		_, err = fs.Put(common.PutPrm{Address: a, RawData: data})
		require.NoError(t, err)
		store[a.EncodeToString()] = data
	}

	t.Run("get", func(t *testing.T) {
		for _, a := range addrs {
			actual, err := fs.Get(common.GetPrm{Address: a})
			require.NoError(t, err)
			require.Equal(t, store[a.EncodeToString()], actual.RawData)
		}

		_, err := fs.Get(common.GetPrm{Address: oidtest.Address()})
		require.Error(t, err)
	})

	t.Run("exists", func(t *testing.T) {
		for _, a := range addrs {
			res, err := fs.Exists(common.ExistsPrm{Address: a})
			require.NoError(t, err)
			require.True(t, res.Exists)
		}

		res, err := fs.Exists(common.ExistsPrm{Address: oidtest.Address()})
		require.NoError(t, err)
		require.False(t, res.Exists)
	})

	t.Run("iterate", func(t *testing.T) {
		n := 0
		var iterationPrm common.IteratePrm
		iterationPrm.Handler = func(elem common.IterationElement) error {
			n++
			addr := elem.Address.EncodeToString()
			expected, ok := store[addr]
			require.True(t, ok, "object %s was not found", addr)
			require.Equal(t, elem.ObjectData, expected)
			return nil
		}

		_, err := fs.Iterate(iterationPrm)

		require.NoError(t, err)
		require.Equal(t, count, n)

		t.Run("leave early", func(t *testing.T) {
			n := 0
			errStop := errors.New("stop")

			iterationPrm.Handler = func(_ common.IterationElement) error {
				if n++; n == count-1 {
					return errStop
				}
				return nil
			}

			_, err := fs.Iterate(iterationPrm)

			require.ErrorIs(t, err, errStop)
			require.Equal(t, count-1, n)
		})

		t.Run("ignore errors", func(t *testing.T) {
			n := 0

			// Unreadable directory.
			require.NoError(t, os.Mkdir(filepath.Join(fs.RootPath, "ZZ"), 0))

			// Unreadable file.
			p := fs.treePath(oidtest.Address())
			require.NoError(t, util.MkdirAllX(filepath.Dir(p), fs.Permissions))
			require.NoError(t, os.WriteFile(p, []byte{1, 2, 3}, 0))

			// Invalid address.
			p = fs.treePath(oidtest.Address()) + ".invalid"
			require.NoError(t, util.MkdirAllX(filepath.Dir(p), fs.Permissions))
			require.NoError(t, os.WriteFile(p, []byte{1, 2, 3}, fs.Permissions))

			iterationPrm.IgnoreErrors = true
			iterationPrm.Handler = func(_ common.IterationElement) error {
				n++
				return nil
			}

			_, err := fs.Iterate(iterationPrm)
			require.NoError(t, err)
			require.Equal(t, count, n)

			t.Run("error from handler is returned", func(t *testing.T) {
				expectedErr := errors.New("expected error")
				n := 0

				iterationPrm.Handler = func(_ common.IterationElement) error {
					n++
					if n == count/2 { // process some iterations
						return expectedErr
					}
					return nil
				}

				_, err := fs.Iterate(iterationPrm)
				require.ErrorIs(t, err, expectedErr)
				require.Equal(t, count/2, n)
			})
		})
	})

	t.Run("delete", func(t *testing.T) {
		_, err := fs.Delete(common.DeletePrm{Address: addrs[0]})
		require.NoError(t, err)

		res, err := fs.Exists(common.ExistsPrm{Address: addrs[0]})
		require.NoError(t, err)
		require.False(t, res.Exists)

		res, err = fs.Exists(common.ExistsPrm{Address: addrs[1]})
		require.NoError(t, err)
		require.True(t, res.Exists)

		_, err = fs.Delete(common.DeletePrm{Address: oidtest.Address()})
		require.Error(t, err)
	})
}
