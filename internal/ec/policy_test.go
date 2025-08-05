package ec_test

import (
	"fmt"
	"slices"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/stretchr/testify/require"
)

func TestNodeSequenceForPartReading(t *testing.T) {
	for _, tc := range []struct {
		partIdx    int
		totalParts int
		nodes      int
		exp        []int
	}{
		{partIdx: 0, totalParts: 1, nodes: 1, exp: []int{0}},
		{partIdx: 0, totalParts: 5, nodes: 5, exp: []int{0, 1, 2, 3, 4}},
		{partIdx: 2, totalParts: 5, nodes: 5, exp: []int{2, 3, 4, 0, 1}},
		{partIdx: 4, totalParts: 5, nodes: 5, exp: []int{4, 0, 1, 2, 3}},
		{partIdx: 0, totalParts: 5, nodes: 10, exp: []int{0, 5, 1, 6, 2, 7, 3, 8, 4, 9}},
		{partIdx: 2, totalParts: 5, nodes: 10, exp: []int{2, 7, 3, 8, 4, 9, 0, 5, 1, 6}},
		{partIdx: 0, totalParts: 5, nodes: 15, exp: []int{0, 5, 10, 1, 6, 11, 2, 7, 12, 3, 8, 13, 4, 9, 14}},
		{partIdx: 2, totalParts: 5, nodes: 15, exp: []int{2, 7, 12, 3, 8, 13, 4, 9, 14, 0, 5, 10, 1, 6, 11}},
		{partIdx: 4, totalParts: 5, nodes: 15, exp: []int{4, 9, 14, 0, 5, 10, 1, 6, 11, 2, 7, 12, 3, 8, 13}},
	} {
		t.Run(fmt.Sprintf("part=%d,parts=%d,nodes=%d", tc.partIdx, tc.totalParts, tc.nodes), func(t *testing.T) {
			require.Equal(t, tc.exp, slices.Collect(iec.NodeSequenceForPart(tc.partIdx, tc.totalParts, tc.nodes)))

			var collected []int
			for i := range iec.NodeSequenceForPart(tc.partIdx, tc.totalParts, tc.nodes) {
				collected = append(collected, i)
				break
			}
			require.Len(t, collected, 1)
			require.Equal(t, collected[0], tc.exp[0])
		})
	}
}
