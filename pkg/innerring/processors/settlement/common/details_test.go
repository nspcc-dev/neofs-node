package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasicIncomeDistributionDetails(t *testing.T) {
	var n uint64 = 1994 // 0x7CA
	exp := []byte{0x42, 0xCA, 0x07, 0, 0, 0, 0, 0, 0}
	got := BasicIncomeDistributionDetails(n)
	require.Equal(t, exp, got)
}
