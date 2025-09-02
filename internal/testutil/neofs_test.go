package testutil_test

import (
	"slices"
	"strconv"
	"strings"
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

		for j, netAddr := range slices.Collect(s[i].NetworkEndpoints()) {
			ps, ok := strings.CutPrefix(netAddr, "localhost:")
			require.True(t, ok)

			p, err := strconv.ParseUint(ps, 10, 16)
			require.NoError(t, err)

			require.EqualValues(t, 10_000+2*i+j, p)
		}
	}
	require.Len(t, m, len(s))
}
