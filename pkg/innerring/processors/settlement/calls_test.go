package settlement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Regression test for M39: two Alphabet nodes computing settlementVUB
// independently - at different real times, since settlement is driven by
// each node's own local timer rather than a shared triggering transaction -
// must derive the exact same ValidUntilBlock whenever they agree on the
// chain-recorded epoch start block and epoch duration (which, unlike each
// node's own observed current height, is identical for all of them no
// matter when it's queried). Otherwise their notary signatures build
// different main transactions and can never combine into one 2/3+1
// transaction, silently losing that epoch's settlement.
func TestComputeSettlementVUB(t *testing.T) {
	const startBlock uint32 = 12_345
	const epochDuration uint64 = 240

	a := computeSettlementVUB(startBlock, epochDuration)
	b := computeSettlementVUB(startBlock, epochDuration)

	require.Equal(t, a, b, "same chain-recorded inputs must always yield the same VUB")
	require.Equal(t, uint64(startBlock)+2*epochDuration, uint64(a))
	require.Greater(t, a, startBlock, "margin must push VUB comfortably past the epoch's start")
}
