package object

import (
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/stretchr/testify/require"
)

type mutuallyAuthenticatedClient bool

func (x mutuallyAuthenticatedClient) IsMutuallyAuthenticated() bool {
	return bool(x)
}

func TestShouldSignOutgoingRequest(t *testing.T) {
	requestWithTTL := func(ttl uint32) *protoobject.GetRequest {
		return &protoobject.GetRequest{MetaHeader: &protosession.RequestMetaHeader{Ttl: ttl}}
	}

	require.False(t, shouldSignOutgoingRequest(mutuallyAuthenticatedClient(true), requestWithTTL(1)))
	require.True(t, shouldSignOutgoingRequest(mutuallyAuthenticatedClient(true), requestWithTTL(2)))
	require.True(t, shouldSignOutgoingRequest(mutuallyAuthenticatedClient(true), new(protoobject.GetRequest)))
	require.True(t, shouldSignOutgoingRequest(mutuallyAuthenticatedClient(false), requestWithTTL(1)))
	require.True(t, shouldSignOutgoingRequest(struct{}{}, requestWithTTL(1)))
}

func TestIterateSearchableContainerNodes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		nodeSets [][]netmap.NodeInfo
		repRules []uint
		ecRules  []iec.Rule
		allNodes bool
		expected int
	}{
		{
			name:     "EC with candidate buffer",
			nodeSets: [][]netmap.NodeInfo{make([]netmap.NodeInfo, 7)},
			ecRules:  []iec.Rule{{DataPartNum: 3, ParityPartNum: 1}},
			expected: 5,
		},
		{
			name:     "EC with smaller candidate buffer",
			nodeSets: [][]netmap.NodeInfo{make([]netmap.NodeInfo, 6)},
			ecRules:  []iec.Rule{{DataPartNum: 3, ParityPartNum: 1}},
			expected: 4,
		},
		{
			name:     "EC without candidate buffer",
			nodeSets: [][]netmap.NodeInfo{make([]netmap.NodeInfo, 4)},
			ecRules:  []iec.Rule{{DataPartNum: 3, ParityPartNum: 1}},
			expected: 2,
		},
		{
			name:     "EC candidate set smaller than required",
			nodeSets: [][]netmap.NodeInfo{make([]netmap.NodeInfo, 1)},
			ecRules:  []iec.Rule{{DataPartNum: 3, ParityPartNum: 1}},
			expected: 1,
		},
		{
			name:     "all EC nodes requested",
			nodeSets: [][]netmap.NodeInfo{make([]netmap.NodeInfo, 7)},
			ecRules:  []iec.Rule{{DataPartNum: 3, ParityPartNum: 1}},
			allNodes: true,
			expected: 7,
		},
		{
			name: "replica nodes are not reduced",
			nodeSets: [][]netmap.NodeInfo{
				make([]netmap.NodeInfo, 7),
				make([]netmap.NodeInfo, 7),
			},
			repRules: []uint{2},
			ecRules:  []iec.Rule{{DataPartNum: 3, ParityPartNum: 1}},
			expected: 12,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got int
			iterateSearchableContainerNodes(tc.nodeSets, tc.repRules, tc.ecRules, tc.allNodes, func(netmap.NodeInfo) bool {
				got++
				return true
			})
			require.Equal(t, tc.expected, got)
		})
	}

	t.Run("stops when callback returns false", func(t *testing.T) {
		var got int
		iterateSearchableContainerNodes(
			[][]netmap.NodeInfo{make([]netmap.NodeInfo, 7)}, nil,
			[]iec.Rule{{DataPartNum: 3, ParityPartNum: 1}}, false,
			func(netmap.NodeInfo) bool {
				got++
				return got < 3
			},
		)
		require.Equal(t, 3, got)
	})
}
