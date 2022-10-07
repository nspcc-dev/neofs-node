package policer

import (
	"testing"

	netmaptest "github.com/nspcc-dev/neofs-sdk-go/netmap/test"
	"github.com/stretchr/testify/require"
)

func TestNodeCache(t *testing.T) {
	cache := newNodeCache()
	node := netmaptest.NodeInfo()

	require.Negative(t, cache.processStatus(node))

	cache.SubmitSuccessfulReplication(node)
	require.Zero(t, cache.processStatus(node))

	cache.submitReplicaCandidate(node)
	require.Positive(t, cache.processStatus(node))

	cache.submitReplicaHolder(node)
	require.Zero(t, cache.processStatus(node))
}
