package timer_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/morph/timer"
	"github.com/stretchr/testify/require"
)

func tickN(t *timer.BlockTimer, n uint32) {
	for i := uint32(0); i < n; i++ {
		t.Tick()
	}
}

func TestBlockTimer(t *testing.T) {
	blockDur := uint32(10)
	baseCallCounter := uint32(0)

	bt := timer.NewBlockTimer(timer.StaticBlockMeter(blockDur), func() {
		baseCallCounter++
	})

	require.NoError(t, bt.Reset())

	intervalNum := uint32(7)

	tickN(bt, intervalNum*blockDur)

	require.Equal(t, intervalNum, uint32(baseCallCounter))

	// add half-interval handler
	halfCallCounter := uint32(0)

	bt.OnDelta(1, 2, func() {
		halfCallCounter++
	})

	// add double interval handler
	doubleCallCounter := uint32(0)

	bt.OnDelta(2, 1, func() {
		doubleCallCounter++
	})

	require.NoError(t, bt.Reset())

	baseCallCounter = 0
	intervalNum = 20

	tickN(bt, intervalNum*blockDur)

	require.Equal(t, intervalNum, uint32(halfCallCounter))
	require.Equal(t, intervalNum, uint32(baseCallCounter))
	require.Equal(t, intervalNum/2, uint32(doubleCallCounter))
}

func TestDeltaPulse(t *testing.T) {
	blockDur := uint32(9)
	baseCallCounter := uint32(0)

	bt := timer.NewBlockTimer(timer.StaticBlockMeter(blockDur), func() {
		baseCallCounter++
	})

	deltaCallCounter := uint32(0)

	div := uint32(3)

	bt.OnDelta(1, div, func() {
		deltaCallCounter++
	}, timer.WithPulse())

	require.NoError(t, bt.Reset())

	intervalNum := uint32(7)

	tickN(bt, intervalNum*blockDur)

	require.Equal(t, intervalNum, uint32(baseCallCounter))
	require.Equal(t, intervalNum*div, uint32(deltaCallCounter))
}

func TestDeltaReset(t *testing.T) {
	blockDur := uint32(6)
	baseCallCounter := 0

	bt := timer.NewBlockTimer(timer.StaticBlockMeter(blockDur), func() {
		baseCallCounter++
	})

	detlaCallCounter := 0

	bt.OnDelta(1, 3, func() {
		detlaCallCounter++
	})

	require.NoError(t, bt.Reset())

	tickN(bt, 6)

	require.Equal(t, 1, baseCallCounter)
	require.Equal(t, 1, detlaCallCounter)

	require.NoError(t, bt.Reset())

	tickN(bt, 3)

	require.Equal(t, 2, detlaCallCounter)
}
