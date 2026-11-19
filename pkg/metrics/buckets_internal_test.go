package metrics

import (
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestReadPathBuckets(t *testing.T) {
	require.True(t, slices.IsSorted(readPathBuckets))
	require.Len(t, slices.Compact(slices.Clone(readPathBuckets)), len(readPathBuckets))
	// Nothing that read the default bounds loses one.
	for _, b := range prometheus.DefBuckets {
		require.Contains(t, readPathBuckets, b)
	}
	require.Less(t, readPathBuckets[0], prometheus.DefBuckets[0])
}
