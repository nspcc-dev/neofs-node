package fstree

import (
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/util"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
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

		data := make([]byte, 10)
		_, _ = rand.Read(data[:])
		require.NoError(t, fs.Put(a, data))
		store[a.EncodeToString()] = data
	}

	t.Run("get", func(t *testing.T) {
		for _, a := range addrs {
			actual, err := fs.Get(a)
			require.NoError(t, err)
			require.Equal(t, store[a.EncodeToString()], actual)
		}

		_, err := fs.Get(oidtest.Address())
		require.Error(t, err)
	})

	t.Run("exists", func(t *testing.T) {
		for _, a := range addrs {
			_, err := fs.Exists(a)
			require.NoError(t, err)
		}

		_, err := fs.Exists(oidtest.Address())
		require.Error(t, err)
	})

	t.Run("iterate", func(t *testing.T) {
		n := 0
		err := fs.Iterate(new(IterationPrm).WithHandler(func(addr oid.Address, data []byte) error {
			n++
			expected, ok := store[addr.EncodeToString()]
			require.True(t, ok, "object %s was not found", addr.EncodeToString())
			require.Equal(t, data, expected)
			return nil
		}))

		require.NoError(t, err)
		require.Equal(t, count, n)

		t.Run("leave early", func(t *testing.T) {
			n := 0
			errStop := errors.New("stop")
			err := fs.Iterate(new(IterationPrm).WithHandler(func(addr oid.Address, data []byte) error {
				if n++; n == count-1 {
					return errStop
				}
				return nil
			}))

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

			err := fs.Iterate(new(IterationPrm).WithHandler(func(addr oid.Address, data []byte) error {
				n++
				return nil
			}).WithIgnoreErrors(true))
			require.NoError(t, err)
			require.Equal(t, count, n)

			t.Run("error from handler is returned", func(t *testing.T) {
				expectedErr := errors.New("expected error")
				n := 0
				err := fs.Iterate(new(IterationPrm).WithHandler(func(addr oid.Address, data []byte) error {
					n++
					if n == count/2 { // process some iterations
						return expectedErr
					}
					return nil
				}).WithIgnoreErrors(true))
				require.ErrorIs(t, err, expectedErr)
				require.Equal(t, count/2, n)
			})
		})
	})

	t.Run("delete", func(t *testing.T) {
		require.NoError(t, fs.Delete(addrs[0]))

		_, err := fs.Exists(addrs[0])
		require.Error(t, err)

		_, err = fs.Exists(addrs[1])
		require.NoError(t, err)

		require.Error(t, fs.Delete(oidtest.Address()))
	})
}
