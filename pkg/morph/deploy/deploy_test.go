package deploy

import (
	"math"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/stretchr/testify/require"
)

func TestNeoFSRuntimeTransactionModifier(t *testing.T) {
	t.Run("invalid invocation result state", func(t *testing.T) {
		var res result.Invoke
		res.State = "FAULT" // any non-HALT

		err := neoFSRuntimeTransactionModifier(func() uint32 { return 0 })(&res, new(transaction.Transaction))
		require.Error(t, err)
	})

	var validRes result.Invoke
	validRes.State = "HALT"

	for _, tc := range []struct {
		curHeight     uint32
		expectedNonce uint32
		expectedVUB   uint32
	}{
		{curHeight: 0, expectedNonce: 0, expectedVUB: 100},
		{curHeight: 1, expectedNonce: 0, expectedVUB: 100},
		{curHeight: 99, expectedNonce: 0, expectedVUB: 100},
		{curHeight: 100, expectedNonce: 100, expectedVUB: 200},
		{curHeight: 199, expectedNonce: 100, expectedVUB: 200},
		{curHeight: 200, expectedNonce: 200, expectedVUB: 300},
		{curHeight: math.MaxUint32 - 50, expectedNonce: 100 * (math.MaxUint32 / 100), expectedVUB: math.MaxUint32},
	} {
		m := neoFSRuntimeTransactionModifier(func() uint32 { return tc.curHeight })

		var tx transaction.Transaction

		err := m(&validRes, &tx)
		require.NoError(t, err, tc)
		require.EqualValues(t, tc.expectedNonce, tx.Nonce, tc)
		require.EqualValues(t, tc.expectedVUB, tx.ValidUntilBlock, tc)
	}
}
