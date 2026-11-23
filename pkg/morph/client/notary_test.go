package client

import (
	"math"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/stretchr/testify/require"
)

func TestCalculateNotaryDepositAmount(t *testing.T) {
	const (
		gasMul = 3
		gasDiv = 2
	)

	t.Run("notary balance at threshold returns zero", func(t *testing.T) {
		amount, err := calculateNotaryDepositAmount(300, 100, gasMul, gasDiv)
		require.NoError(t, err)
		require.Equal(t, fixedn.Fixed8(0), amount)
	})

	t.Run("notary balance above threshold returns zero", func(t *testing.T) {
		amount, err := calculateNotaryDepositAmount(500, 100, gasMul, gasDiv)
		require.NoError(t, err)
		require.Equal(t, fixedn.Fixed8(0), amount)
	})

	t.Run("notary balance below threshold returns share of gas balance", func(t *testing.T) {
		amount, err := calculateNotaryDepositAmount(299, 100, gasMul, gasDiv)
		require.NoError(t, err)
		require.Equal(t, fixedn.Fixed8(50), amount)
	})

	t.Run("zero gas balance and zero notary balance returns error", func(t *testing.T) {
		_, err := calculateNotaryDepositAmount(0, 0, gasMul, gasDiv)
		require.Error(t, err)
	})

	t.Run("zero gas balance with existing notary balance returns zero", func(t *testing.T) {
		amount, err := calculateNotaryDepositAmount(100, 0, gasMul, gasDiv)
		require.NoError(t, err)
		require.Equal(t, fixedn.Fixed8(0), amount)
	})

	t.Run("gas balance below divisor returns zero", func(t *testing.T) {
		amount, err := calculateNotaryDepositAmount(0, 1, gasMul, gasDiv)
		require.NoError(t, err)
		require.Equal(t, fixedn.Fixed8(0), amount)
	})
}

func TestShouldSkipNotaryDeposit(t *testing.T) {
	t.Run("zero amount with endless deposit skips", func(t *testing.T) {
		require.True(t, shouldSkipNotaryDeposit(0, math.MaxUint32, math.MaxUint32))
	})

	t.Run("zero amount with finite deposit upgrades to endless", func(t *testing.T) {
		require.False(t, shouldSkipNotaryDeposit(0, 5760, math.MaxUint32))
	})

	t.Run("zero amount with no existing deposit does not skip", func(t *testing.T) {
		require.False(t, shouldSkipNotaryDeposit(0, 0, math.MaxUint32))
		require.False(t, shouldSkipNotaryDeposit(0, 0, 1000))
	})

	t.Run("zero amount with main-chain deposit covering target skips", func(t *testing.T) {
		require.True(t, shouldSkipNotaryDeposit(0, 2000, 2000))
		require.True(t, shouldSkipNotaryDeposit(0, 2000, 1500))
	})

	t.Run("zero amount with main-chain deposit below target extends", func(t *testing.T) {
		require.False(t, shouldSkipNotaryDeposit(0, 2000, 2001))
	})

	t.Run("positive amount never skips", func(t *testing.T) {
		require.False(t, shouldSkipNotaryDeposit(1, math.MaxUint32, math.MaxUint32))
		require.False(t, shouldSkipNotaryDeposit(100, 2000, 1500))
	})
}
