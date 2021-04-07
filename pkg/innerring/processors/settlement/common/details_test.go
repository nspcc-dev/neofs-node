package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuditSettlementDetails(t *testing.T) {
	var n uint64 = 1994 // 0x7CA
	exp := []byte{0x40, 0xCA, 0x07, 0, 0, 0, 0, 0, 0}
	got := AuditSettlementDetails(n)
	require.Equal(t, exp, got)
}

func TestBasicIncomeCollectionDetails(t *testing.T) {
	var n uint64 = 1994 // 0x7CA
	exp := []byte{0x41, 0xCA, 0x07, 0, 0, 0, 0, 0, 0}
	got := BasicIncomeCollectionDetails(n)
	require.Equal(t, exp, got)
}

func TestBasicIncomeDistributionDetails(t *testing.T) {
	var n uint64 = 1994 // 0x7CA
	exp := []byte{0x42, 0xCA, 0x07, 0, 0, 0, 0, 0, 0}
	got := BasicIncomeDistributionDetails(n)
	require.Equal(t, exp, got)
}
