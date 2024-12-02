package writecache_test

import (
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestCache_InitReadOnly(t *testing.T) {
	dir := t.TempDir()

	wc := writecache.New(
		writecache.WithPath(dir),
	)

	// open in rw mode first to create underlying BoltDB with some object
	// (otherwise 'bad file descriptor' error on Open occurs)
	err := wc.Open(false)
	require.NoError(t, err)

	err = wc.Init()
	require.NoError(t, err)

	obj := objecttest.Object()

	err = wc.Put(objectcore.AddressOf(&obj), &obj, nil)
	require.NoError(t, err)

	err = wc.Close()
	require.NoError(t, err)

	// try Init in read-only mode
	err = wc.Open(true)
	require.NoError(t, err)

	t.Cleanup(func() { wc.Close() })

	err = wc.Init()
	require.NoError(t, err)
}
