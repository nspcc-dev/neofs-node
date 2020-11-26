package blobovnicza

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSizes(t *testing.T) {
	for _, item := range []struct {
		sz uint64 // object size

		upperBound uint64 // upper bound of expected range
	}{
		{
			sz:         0,
			upperBound: firstBucketBound,
		},
		{
			sz:         firstBucketBound,
			upperBound: firstBucketBound,
		},
		{
			sz:         firstBucketBound + 1,
			upperBound: 2 * firstBucketBound,
		},
		{
			sz:         2 * firstBucketBound,
			upperBound: 2 * firstBucketBound,
		},
		{
			sz:         2*firstBucketBound + 1,
			upperBound: 4 * firstBucketBound,
		},
	} {
		require.Equal(t, bucketKeyFromBounds(item.upperBound), bucketForSize(item.sz))
	}
}
