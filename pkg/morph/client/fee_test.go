package client

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/stretchr/testify/require"
)

func TestFees(t *testing.T) {
	var v fees

	const method = "some method"

	var (
		fee fixedn.Fixed8
		def = fixedn.Fixed8(13)
	)

	v.setDefault(def)

	fee = v.feeForMethod(method)
	require.True(t, fee.Equal(def))

	const customFee = fixedn.Fixed8(10)

	v.setFeeForMethod(method, customFee)

	fee = v.feeForMethod(method)

	require.Equal(t, customFee, fee)
}
