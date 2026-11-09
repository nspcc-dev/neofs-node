package object

import (
	"context"
	"net"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	iprotobuf "github.com/nspcc-dev/neofs-sdk-go/proto/protobuf"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
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

// An attribute-less query whose first filter is not STRING_EQUAL requires the
// single synthesized attribute: MergeSearchResults indexes Attributes[0] of
// such items without checking it is there. Only STRING_EQUAL queries may omit
// it, since conforming peers deliberately clear a value the caller knows.
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

// searchV2OnlyConn serves resp to any SearchV2 call.
func searchV2OnlyConn(t *testing.T, resp *protoobject.SearchV2Response) *grpc.ClientConn {
	lis := bufconn.Listen(32 << 10)

	grpcSrv := grpc.NewServer(grpc.ForceServerCodecV2(iprotobuf.BufferedCodec{}))
	t.Cleanup(grpcSrv.Stop)

	grpcSrv.RegisterService(&grpc.ServiceDesc{
		ServiceName: protoobject.ObjectService_ServiceDesc.ServiceName,
		Methods: []grpc.MethodDesc{
			{
				MethodName: "SearchV2",
				Handler: func(_ any, _ context.Context, _ func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
					return resp, nil
				},
			},
		},
	}, nil)

	go func() { _ = grpcSrv.Serve(lis) }()

	c, err := grpc.NewClient("passthrough:///bufnet", // dialer ignores it; skips DNS
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()), // error otherwise
	)
	require.NoError(t, err) // lib misuse or internal error
	t.Cleanup(func() { _ = c.Close() })

	return c
}

// The validation must be reachable from the caller: an item accepted here
// reaches MergeSearchResults, which indexes Attributes[0] of it.
func TestSearchOnRemoteAddress_AttributelessItem(t *testing.T) {
	reqBody := func(mt protoobject.MatchType) *protoobject.SearchV2Request_Body {
		return &protoobject.SearchV2Request_Body{
			Count:   1,
			Filters: []*protoobject.SearchFilter{{MatchType: mt, Key: "someAttribute"}},
		}
	}
	resp := &protoobject.SearchV2Response{
		Body: &protoobject.SearchV2Response_Body{
			Result: []*protoobject.SearchV2Response_OIDWithMeta{
				{Id: oidtest.ID().ProtoMessage()},
			},
		},
	}

	t.Run("non-equality filter", func(t *testing.T) {
		_, _, err := searchOnRemoteAddress(context.Background(), searchV2OnlyConn(t, resp), nil, reqBody(protoobject.MatchType_NUM_GT))
		require.ErrorContains(t, err, "missing required attribute")
	})

	t.Run("STRING_EQUAL filter", func(t *testing.T) {
		res, more, err := searchOnRemoteAddress(context.Background(), searchV2OnlyConn(t, resp), nil, reqBody(protoobject.MatchType_STRING_EQUAL))
		require.NoError(t, err)
		require.False(t, more)
		require.Len(t, res, 1)
	})
}
