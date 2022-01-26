package fstree

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/util"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/address/test"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func testOID() *oidSDK.ID {
	cs := [sha256.Size]byte{}
	_, _ = rand.Read(cs[:])

	id := oidSDK.NewID()
	id.SetSHA256(cs)

	return id
}

func testAddress() *addressSDK.Address {
	a := addressSDK.NewAddress()
	a.SetObjectID(testOID())
	a.SetContainerID(cidtest.ID())

	return a
}

func TestAddressToString(t *testing.T) {
	addr := testAddress()
	s := stringifyAddress(addr)
	actual, err := addressFromString(s)
	require.NoError(t, err)
	require.Equal(t, addr, actual)
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
	var addrs []*addressSDK.Address

	store := map[string][]byte{}

	for i := 0; i < count; i++ {
		a := testAddress()
		addrs = append(addrs, a)

		data := make([]byte, 10)
		_, _ = rand.Read(data[:])
		require.NoError(t, fs.Put(a, data))
		store[a.String()] = data
	}

	t.Run("get", func(t *testing.T) {
		for _, a := range addrs {
			actual, err := fs.Get(a)
			require.NoError(t, err)
			require.Equal(t, store[a.String()], actual)
		}

		_, err := fs.Get(testAddress())
		require.Error(t, err)
	})

	t.Run("exists", func(t *testing.T) {
		for _, a := range addrs {
			_, err := fs.Exists(a)
			require.NoError(t, err)
		}

		_, err := fs.Exists(testAddress())
		require.Error(t, err)
	})

	t.Run("iterate", func(t *testing.T) {
		n := 0
		err := fs.Iterate(new(IterationPrm).WithHandler(func(addr *addressSDK.Address, data []byte) error {
			n++
			expected, ok := store[addr.String()]
			require.True(t, ok, "object %s was not found", addr.String())
			require.Equal(t, data, expected)
			return nil
		}))

		require.NoError(t, err)
		require.Equal(t, count, n)

		t.Run("leave early", func(t *testing.T) {
			n := 0
			errStop := errors.New("stop")
			err := fs.Iterate(new(IterationPrm).WithHandler(func(addr *addressSDK.Address, data []byte) error {
				if n++; n == count-1 {
					return errStop
				}
				return nil
			}))

			require.True(t, errors.Is(err, errStop))
			require.Equal(t, count-1, n)
		})

		t.Run("ignore errors", func(t *testing.T) {
			n := 0

			// Unreadable directory.
			require.NoError(t, os.Mkdir(filepath.Join(fs.RootPath, "ZZ"), 0))

			// Unreadable file.
			p := fs.treePath(objecttest.Address())
			require.NoError(t, util.MkdirAllX(filepath.Dir(p), fs.Permissions))
			require.NoError(t, os.WriteFile(p, []byte{1, 2, 3}, 0))

			// Invalid address.
			p = fs.treePath(objecttest.Address()) + ".invalid"
			require.NoError(t, util.MkdirAllX(filepath.Dir(p), fs.Permissions))
			require.NoError(t, os.WriteFile(p, []byte{1, 2, 3}, fs.Permissions))

			err := fs.Iterate(new(IterationPrm).WithHandler(func(addr *addressSDK.Address, data []byte) error {
				n++
				return nil
			}).WithIgnoreErrors(true))
			require.NoError(t, err)
			require.Equal(t, count, n)

			t.Run("error from handler is returned", func(t *testing.T) {
				expectedErr := errors.New("expected error")
				n := 0
				err := fs.Iterate(new(IterationPrm).WithHandler(func(addr *addressSDK.Address, data []byte) error {
					n++
					if n == count/2 { // process some iterations
						return expectedErr
					}
					return nil
				}).WithIgnoreErrors(true))
				require.True(t, errors.Is(err, expectedErr), "got: %v")
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

		require.Error(t, fs.Delete(testAddress()))
	})
}
