package object

import (
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"github.com/stretchr/testify/require"
)

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

// Regression test: a remote peer's SearchV2 response item must carry
// exactly one attribute for an attribute-less query whose first filter is not
// STRING_EQUAL - that single synthesized attribute is what MergeSearchResults
// indexes (sets[i][0].Attributes[0]) to compare and page results across
// peers. The wire format's own doc comment on SearchV2Response_OIDWithMeta.
// Attributes says as much ("this field carry value of the first filtered
// attribute"). Before this fix, a response with zero attributes for a
// non-equality filter was accepted here, which let a malicious or
// non-conforming peer crash the merge with an out-of-range index - the exact
// panic class already hit once in production for the STRING_EQUAL case
// (fixed by a059fd39f) and never closed for every other filter type.
func TestValidateSearchResultItem(t *testing.T) {
	id := &refs.ObjectID{Value: make([]byte, 32)}

	t.Run("nil element", func(t *testing.T) {
		require.ErrorContains(t, validateSearchResultItem(nil, false, 0, 0), "nil element")
	})

	t.Run("missing ID", func(t *testing.T) {
		r := &protoobject.SearchV2Response_OIDWithMeta{}
		require.ErrorContains(t, validateSearchResultItem(r, false, 1, 0), "missing ID")
	})

	t.Run("attributed query: attribute count must match exactly", func(t *testing.T) {
		r := &protoobject.SearchV2Response_OIDWithMeta{Id: id, Attributes: []string{"a"}}
		require.NoError(t, validateSearchResultItem(r, false, 1, 0))

		r.Attributes = nil
		require.ErrorContains(t, validateSearchResultItem(r, false, 1, 0), "wrong attribute count")

		r.Attributes = []string{"a", "b"}
		require.ErrorContains(t, validateSearchResultItem(r, false, 1, 0), "wrong attribute count")
	})

	t.Run("attribute-less query, STRING_EQUAL filter: zero or one attribute both accepted", func(t *testing.T) {
		r := &protoobject.SearchV2Response_OIDWithMeta{Id: id}
		require.NoError(t, validateSearchResultItem(r, true, 0, protoobject.MatchType_STRING_EQUAL))

		r.Attributes = []string{"v"}
		require.NoError(t, validateSearchResultItem(r, true, 0, protoobject.MatchType_STRING_EQUAL))

		r.Attributes = []string{"v", "extra"}
		require.ErrorContains(t, validateSearchResultItem(r, true, 0, protoobject.MatchType_STRING_EQUAL), "wrong attribute count")
	})

	t.Run("attribute-less query, non-equality filter: exactly one attribute required", func(t *testing.T) {
		r := &protoobject.SearchV2Response_OIDWithMeta{Id: id, Attributes: []string{"v"}}
		require.NoError(t, validateSearchResultItem(r, true, 0, protoobject.MatchType_NUM_GT))

		r.Attributes = nil
		require.ErrorContains(t, validateSearchResultItem(r, true, 0, protoobject.MatchType_NUM_GT), "missing required attribute")

		r.Attributes = []string{"v", "extra"}
		require.ErrorContains(t, validateSearchResultItem(r, true, 0, protoobject.MatchType_NUM_GT), "wrong attribute count")
	})
}
