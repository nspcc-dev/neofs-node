package implementations

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/stretchr/testify/require"
)

func TestBalanceOfParams(t *testing.T) {
	s := BalanceOfParams{}

	owner := refs.OwnerID{1, 2, 3}
	s.SetOwnerID(owner)

	require.Equal(t, owner, s.OwnerID())
}

func TestBalanceOfResult(t *testing.T) {
	s := BalanceOfResult{}

	amount := int64(100)
	s.SetAmount(amount)

	require.Equal(t, amount, s.Amount())
}

func TestDecimalsResult(t *testing.T) {
	s := DecimalsResult{}

	dec := int64(100)
	s.SetDecimals(dec)

	require.Equal(t, dec, s.Decimals())
}
