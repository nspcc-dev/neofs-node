package blobovnicza

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestBlobovniczaIterate(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "blob")
	b := New(WithPath(filename))
	require.NoError(t, b.Open())
	require.NoError(t, b.Init())

	data := [][]byte{{0, 1, 2, 3}, {5, 6, 7, 8}}
	addr := objecttest.Address()
	_, err := b.Put(&PutPrm{addr: addr, objData: data[0]})
	require.NoError(t, err)

	require.NoError(t, b.boltDB.Update(func(tx *bbolt.Tx) error {
		buck := tx.Bucket(bucketKeyFromBounds(firstBucketBound))
		return buck.Put([]byte("invalid address"), data[1])
	}))

	seen := make([][]byte, 0, 2)
	inc := func(e IterationElement) error {
		seen = append(seen, slice.Copy(e.data))
		return nil
	}

	_, err = b.Iterate(IteratePrm{handler: inc})
	require.NoError(t, err)
	require.ElementsMatch(t, seen, data)

	seen = seen[:0]
	_, err = b.Iterate(IteratePrm{handler: inc, decodeAddresses: true})
	require.Error(t, err)

	seen = seen[:0]
	_, err = b.Iterate(IteratePrm{handler: inc, decodeAddresses: true, ignoreErrors: true})
	require.NoError(t, err)
	require.ElementsMatch(t, seen, data[:1])

	seen = seen[:0]
	expectedErr := errors.New("stop iteration")
	_, err = b.Iterate(IteratePrm{
		decodeAddresses: true,
		handler:         func(IterationElement) error { return expectedErr },
		ignoreErrors:    true,
	})
	require.True(t, errors.Is(err, expectedErr), "got: %v")
}
