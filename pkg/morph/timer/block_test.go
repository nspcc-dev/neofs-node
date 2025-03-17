package timer_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/morph/timer"
	"github.com/stretchr/testify/require"
)

func tickN(t *timer.BlockTimer, n uint32) {
	for range n {
		t.Tick(0)
	}
}

// This test emulates inner ring handling of a new epoch and a new block.
// "resetting" consists of ticking the current height as well and invoking `Reset`.
func TestIRBlockTimer_Reset(t *testing.T) {
	var baseCounter [2]int
	blockDur := uint32(3)

	bt1 := timer.NewBlockTimer(
		func() (uint32, error) { return blockDur, nil },
		func() { baseCounter[0]++ })
	bt2 := timer.NewBlockTimer(
		func() (uint32, error) { return blockDur, nil },
		func() { baseCounter[1]++ })

	require.NoError(t, bt1.Reset())
	require.NoError(t, bt2.Reset())

	run := func(bt *timer.BlockTimer, direct bool) {
		if direct {
			bt.Tick(1)
			require.NoError(t, bt.Reset())
			bt.Tick(1)
		} else {
			bt.Tick(1)
			bt.Tick(1)
			require.NoError(t, bt.Reset())
		}
		bt.Tick(2)
		bt.Tick(3)
	}

	run(bt1, true)
	run(bt2, false)
	require.Equal(t, baseCounter[0], baseCounter[1])
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

func TestNewOneTickTimer(t *testing.T) {
	blockDur := uint32(1)
	baseCallCounter := 0

	bt := timer.NewOneTickTimer(timer.StaticBlockMeter(blockDur), func() {
		baseCallCounter++
	})
	require.NoError(t, bt.Reset())

	tickN(bt, 10)
	require.Equal(t, 1, baseCallCounter) // happens once no matter what

	t.Run("zero duration", func(t *testing.T) {
		blockDur = uint32(0)
		baseCallCounter = 0

		bt = timer.NewOneTickTimer(timer.StaticBlockMeter(blockDur), func() {
			baseCallCounter++
		})
		require.NoError(t, bt.Reset())

		tickN(bt, 10)
		require.Equal(t, 1, baseCallCounter)
	})

	t.Run("delta without pulse", func(t *testing.T) {
		blockDur = uint32(10)
		baseCallCounter = 0

		bt = timer.NewOneTickTimer(timer.StaticBlockMeter(blockDur), func() {
			baseCallCounter++
		})

		detlaCallCounter := 0

		bt.OnDelta(1, 10, func() {
			detlaCallCounter++
		})

		require.NoError(t, bt.Reset())

		tickN(bt, 10)
		require.Equal(t, 1, baseCallCounter)
		require.Equal(t, 1, detlaCallCounter)

		tickN(bt, 10) // 10 more ticks must not affect counters
		require.Equal(t, 1, baseCallCounter)
		require.Equal(t, 1, detlaCallCounter)
	})
}

func TestBlockTimer_TickSameHeight(t *testing.T) {
	var baseCounter, deltaCounter int

	blockDur := uint32(2)
	bt := timer.NewBlockTimer(
		func() (uint32, error) { return blockDur, nil },
		func() { baseCounter++ })
	bt.OnDelta(2, 1, func() {
		deltaCounter++
	})
	require.NoError(t, bt.Reset())

	check := func(t *testing.T, h uint32, base, delta int) {
		for range 2 * blockDur {
			bt.Tick(h)
			require.Equal(t, base, baseCounter)
			require.Equal(t, delta, deltaCounter)
		}
	}

	check(t, 1, 0, 0)
	check(t, 2, 1, 0)
	check(t, 3, 1, 0)
	check(t, 4, 2, 1)

	t.Run("works the same way after `Reset()`", func(t *testing.T) {
		t.Run("same block duration", func(t *testing.T) {
			require.NoError(t, bt.Reset())
			baseCounter = 0
			deltaCounter = 0

			check(t, 1, 0, 0)
			check(t, 2, 1, 0)
			check(t, 3, 1, 0)
			check(t, 4, 2, 1)
		})
		t.Run("different block duration", func(t *testing.T) {
			blockDur = 3

			require.NoError(t, bt.Reset())
			baseCounter = 0
			deltaCounter = 0

			check(t, 1, 0, 0)
			check(t, 2, 0, 0)
			check(t, 3, 1, 0)
			check(t, 4, 1, 0)
			check(t, 5, 1, 0)
			check(t, 6, 2, 1)
		})
	})
}

func TestBlockTimer_Stop(t *testing.T) {
	const interval = 5
	var triggers int
	bt := timer.NewBlockTimer(func() (uint32, error) { return interval, nil }, func() { triggers++ })
	require.NoError(t, bt.Reset())

	tickN(bt, 2*interval)
	require.EqualValues(t, 2, triggers)

	triggers = 0
	bt.Stop()

	tickN(bt, 2*interval)
	require.Zero(t, triggers)
}
