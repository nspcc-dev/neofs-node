package testutil_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestNodes(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require.Empty(t, testutil.Nodes(0))
	})

	s := testutil.Nodes(10)

	m := make(map[string]struct{})
	for i := range s {
		m[string(s[i].PublicKey())] = struct{}{}
	}
	require.Len(t, m, len(s))
}
