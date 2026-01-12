package writecache

import (
	"testing"

	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestCache_InitReadOnly(t *testing.T) {
	wc, _ := newCache(t)

	obj := objecttest.Object()

	err := wc.Put(obj.Address(), &obj, nil)
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
