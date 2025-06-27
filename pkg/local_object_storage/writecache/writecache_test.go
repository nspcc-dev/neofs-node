package writecache

import (
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestCache_InitReadOnly(t *testing.T) {
	wc, _ := newCache(t)

	obj := objecttest.Object()

	err := wc.Put(objectcore.AddressOf(&obj), &obj, nil)
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
