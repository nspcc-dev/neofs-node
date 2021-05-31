package audit_test

import (
	"testing"

	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	cidtest "github.com/nspcc-dev/neofs-api-go/pkg/container/id/test"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/audit"
	"github.com/stretchr/testify/require"
)

func TestSelect(t *testing.T) {
	cids := generateContainers(10)

	t.Run("invalid input", func(t *testing.T) {
		require.Empty(t, audit.Select(cids, 0, 0, 0))
	})

	t.Run("even split", func(t *testing.T) {
		const irSize = 5 // every node takes two audit nodes

		m := hitMap(cids)

		for i := 0; i < irSize; i++ {
			s := audit.Select(cids, 0, uint64(i), irSize)
			require.Equal(t, len(cids)/irSize, len(s))

			for _, id := range s {
				n, ok := m[id.String()]
				require.True(t, ok)
				require.Equal(t, 0, n)
				m[id.String()] = 1
			}
		}

		require.True(t, allHit(m))
	})

	t.Run("odd split", func(t *testing.T) {
		const irSize = 3

		m := hitMap(cids)

		for i := 0; i < irSize; i++ {
			s := audit.Select(cids, 0, uint64(i), irSize)

			for _, id := range s {
				n, ok := m[id.String()]
				require.True(t, ok)
				require.Equal(t, 0, n)
				m[id.String()] = 1
			}
		}

		require.True(t, allHit(m))
	})

	t.Run("epoch shift", func(t *testing.T) {
		const irSize = 4

		m := hitMap(cids)

		for i := 0; i < irSize; i++ {
			s := audit.Select(cids, uint64(i), 0, irSize)

			for _, id := range s {
				n, ok := m[id.String()]
				require.True(t, ok)
				require.Equal(t, 0, n)
				m[id.String()] = 1
			}
		}

		require.True(t, allHit(m))
	})
}

func generateContainers(n int) []*cid.ID {
	result := make([]*cid.ID, 0, n)

	for i := 0; i < n; i++ {
		result = append(result, cidtest.Generate())
	}

	return result
}

func hitMap(ids []*cid.ID) map[string]int {
	result := make(map[string]int, len(ids))

	for _, id := range ids {
		result[id.String()] = 0
	}

	return result
}

func allHit(m map[string]int) bool {
	for _, v := range m {
		if v == 0 {
			return false
		}
	}

	return true
}
