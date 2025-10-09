package state_test

import (
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	"github.com/stretchr/testify/require"
)

func newStorage(tb testing.TB) *state.PersistentStorage {
	storage, err := state.NewPersistentStorage(filepath.Join(tb.TempDir(), ".storage"), false)
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_ = storage.Close()
	})

	return storage
}

func TestPersistentStorage_UInt32(t *testing.T) {
	storage := newStorage(t)

	n, err := storage.UInt32([]byte("unset-value"))
	require.NoError(t, err)
	require.EqualValues(t, 0, n)

	err = storage.SetUInt32([]byte("foo"), 10)
	require.NoError(t, err)

	n, err = storage.UInt32([]byte("foo"))
	require.NoError(t, err)
	require.EqualValues(t, 10, n)
}

func TestPersistentStorage_Bytes(t *testing.T) {
	storage := newStorage(t)

	bKey := []byte("bytes")

	bRes, err := storage.Bytes(bKey)
	require.NoError(t, err)
	require.Nil(t, bRes)

	bVal := []byte("Hello, world!")

	err = storage.SetBytes(bKey, bVal)
	require.NoError(t, err)

	bRes, err = storage.Bytes(bKey)
	require.NoError(t, err)
	require.Equal(t, bVal, bRes)
}
