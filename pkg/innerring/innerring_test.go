package innerring

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/core/metachain/meta"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

func TestBuildContainerPlacement(t *testing.T) {
	var rep netmap.ReplicaDescriptor
	rep.SetNumberOfObjects(3)
	firstEC := netmap.NewECRule(12, 4)
	firstEC.SetMinPartNum(14)
	secondEC := netmap.NewECRule(6, 3)
	secondEC.SetMinPartNum(7)
	var initial netmap.InitialPlacementPolicy
	initial.SetReplicaLimits([]uint32{2, 1, 0})
	var global netmap.InitialPlacementPolicy
	global.SetMaxReplicas(2)

	for _, tc := range []struct {
		name    string
		ecRules []netmap.ECRule
		initial *netmap.InitialPlacementPolicy
		want    meta.Placement
	}{
		{name: "REP only", want: meta.Placement{Vectors: []meta.PlacementVector{{REP: 3}}}},
		{
			name:    "EC minima follow REP vectors",
			ecRules: []netmap.ECRule{firstEC, secondEC, netmap.NewECRule(3, 2)},
			want: meta.Placement{Vectors: []meta.PlacementVector{
				{REP: 3}, {REP: 16, MinECParts: 14}, {REP: 9, MinECParts: 7}, {REP: 5},
			}},
		},
		{
			name:    "initial limits preserve minima for enabled EC rules",
			ecRules: []netmap.ECRule{firstEC, secondEC},
			initial: &initial,
			want:    meta.Placement{Vectors: []meta.PlacementVector{{REP: 2}, {REP: 16, MinECParts: 14}, {REP: 0}}},
		},
		{
			name:    "global limit without per-rule limits",
			ecRules: []netmap.ECRule{firstEC, netmap.NewECRule(3, 2)},
			initial: &global,
			want: meta.Placement{MaxReplicas: 2, Vectors: []meta.PlacementVector{
				{REP: 3}, {REP: 16, MinECParts: 14}, {REP: 5, MinECParts: 5},
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var policy netmap.PlacementPolicy
			policy.SetReplicas([]netmap.ReplicaDescriptor{rep})
			policy.SetECRules(tc.ecRules)
			if tc.initial != nil {
				policy.SetInitial(*tc.initial)
			}
			vectors := make([][]netmap.NodeInfo, len(tc.want.Vectors))
			for i := range vectors {
				k, err := keys.NewPrivateKey()
				require.NoError(t, err)
				var node netmap.NodeInfo
				node.SetPublicKey(k.PublicKey().Bytes())
				vectors[i] = []netmap.NodeInfo{node}
				tc.want.Vectors[i].Nodes = keys.PublicKeys{k.PublicKey()}
			}

			got, err := buildContainerPlacement(vectors, policy)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMetadataChainCompatibility(t *testing.T) {
	if config.HFLatestStable != config.HFGorgon {
		t.Fatalf("new unknown stable hardfork (%s), adapt it", config.HFLatestStable)
	}
}
