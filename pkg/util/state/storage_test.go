package state_test

import (
	"path"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	"github.com/stretchr/testify/require"
)

func TestPersistentStorage_UInt32(t *testing.T) {
	storage, err := state.NewPersistentStorage(path.Join(t.TempDir(), ".storage"))
	require.NoError(t, err)
	defer storage.Close()

	n, err := storage.UInt32([]byte("unset-value"))
	require.NoError(t, err)
	require.EqualValues(t, 0, n)

	err = storage.SetUInt32([]byte("foo"), 10)
	require.NoError(t, err)

	n, err = storage.UInt32([]byte("foo"))
	require.NoError(t, err)
	require.EqualValues(t, 10, n)
}
