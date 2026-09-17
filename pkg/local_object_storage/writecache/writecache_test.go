package writecache

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestCache_InitReadOnly(t *testing.T) {
	wc, _ := newCache(t)

	obj := objecttest.Object()

	err := wc.Put(obj.Address(), &obj, []byte{1, 2, 3})
	require.NoError(t, err)

	err = wc.Close()
	require.NoError(t, err)

	// try Init in read-only mode
	err = wc.Open(true)
	require.NoError(t, err)

	t.Cleanup(func() { wc.Close() })

	err = wc.Init(common.ID{})
	require.NoError(t, err)
}

type unimplementedMetrics struct{}

func (unimplementedMetrics) AddWCPutDuration(string, time.Duration) {
	panic("unimplemented")
}

func (unimplementedMetrics) AddWCStreamingPutDuration(string, time.Duration) {
	panic("unimplemented")
}

func (unimplementedMetrics) AddWCFlushSingleDuration(string, time.Duration) {
	panic("unimplemented")
}

func (unimplementedMetrics) AddWCFlushBatchDuration(string, time.Duration) {
	panic("unimplemented")
}

func (unimplementedMetrics) IncWCObjectCount(string) {
	panic("unimplemented")
}

func (unimplementedMetrics) DecWCObjectCount(string) {
	panic("unimplemented")
}

func (unimplementedMetrics) AddWCSize(string, uint64) {
	panic("unimplemented")
}

func (unimplementedMetrics) SetWCSize(string, uint64) {
	panic("unimplemented")
}
