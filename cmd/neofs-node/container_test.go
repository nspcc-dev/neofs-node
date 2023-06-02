package main

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/hrw"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

// TestCase represents collection of placement policy tests for a single node set.
type TestCase struct {
	Name  string            `json:"name"`
	Nodes []netmap.NodeInfo `json:"nodes"`
	Tests map[string]struct {
		Policy    netmap.PlacementPolicy `json:"policy"`
		Pivot     []byte                 `json:"pivot,omitempty"`
		Result    [][]int                `json:"result,omitempty"`
		Error     string                 `json:"error,omitempty"`
		Placement struct {
			Pivot  []byte
			Result [][]int
		} `json:"placement,omitempty"`
	}
}

func TestPlacementVectors_Compatibility(t *testing.T) {
	// That test checks whether the internal code copied the [netmap.NetMap.PlacementVectors].
	// In SDK's 1.0.0-rc-8 a flexible pivot ([]byte) has been removed and no public func/method
	// is available to keep behavior the same, thus that test. Testcases are copied from SDK.
	// It is expected to be removed when a convenient solution is found/merged/released.

	const testsDir = "./placement_vectors_tests"

	f, err := os.Open(testsDir)
	require.NoError(t, err)

	ds, err := f.ReadDir(0)
	require.NoError(t, err)

	for i := range ds {
		bs, err := os.ReadFile(filepath.Join(testsDir, ds[i].Name()))
		require.NoError(t, err)

		var tc TestCase
		require.NoError(t, json.Unmarshal(bs, &tc), "cannot unmarshal %s", ds[i].Name())

		srcNodes := make([]netmap.NodeInfo, len(tc.Nodes))
		copy(srcNodes, tc.Nodes)

		t.Run(tc.Name, func(t *testing.T) {
			var nm netmap.NetMap
			nm.SetNodes(tc.Nodes)

			for name, tt := range tc.Tests {
				t.Run(name, func(t *testing.T) {
					var pivot cid.ID
					copy(pivot[:], tt.Pivot)

					v, err := nm.ContainerNodes(tt.Policy, pivot)
					if tt.Result == nil {
						require.Error(t, err)
						require.Contains(t, err.Error(), tt.Error)
					} else {
						require.NoError(t, err)
						require.Equal(t, srcNodes, tc.Nodes)

						compareNodesIgnoreOrder(t, tt.Result, tc.Nodes, v)

						if tt.Placement.Result != nil {
							placementPivot := [sha256.Size]byte{}
							copy(placementPivot[:], tt.Placement.Pivot)

							res := placementVectors(v, &nm, hrw.Hash(placementPivot[:]))
							compareNodes(t, tt.Placement.Result, tc.Nodes, res)
							require.Equal(t, srcNodes, tc.Nodes)
						}
					}
				})
			}
		})
	}
}

func compareNodes(t testing.TB, expected [][]int, nodes []netmap.NodeInfo, actual [][]netmap.NodeInfo) {
	require.Equal(t, len(expected), len(actual))
	for i := range expected {
		require.Equal(t, len(expected[i]), len(actual[i]))
		for j, index := range expected[i] {
			require.Equal(t, nodes[index], actual[i][j])
		}
	}
}

func compareNodesIgnoreOrder(t testing.TB, expected [][]int, nodes []netmap.NodeInfo, actual [][]netmap.NodeInfo) {
	require.Equal(t, len(expected), len(actual))
	for i := range expected {
		require.Equal(t, len(expected[i]), len(actual[i]))

		var expectedNodes []netmap.NodeInfo
		for _, index := range expected[i] {
			expectedNodes = append(expectedNodes, nodes[index])
		}

		require.ElementsMatch(t, expectedNodes, actual[i])
	}
}
