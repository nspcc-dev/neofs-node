package timers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEpochTimers(t *testing.T) {
	newTimers := func(initDur uint64) (*EpochTimers, *int) {
		var n int
		return NewTimers(EpochTicks{
			InitialEpochDuration: initDur,
			NewEpochTicks:        []Tick{func() { n++ }},
		}), &n
	}

	t.Run("keeps ticking without reset", func(t *testing.T) {
		et, n := newTimers(0)
		et.Reset(1000, 500)

		et.UpdateTime(1400)
		require.Equal(t, 0, *n)
		et.UpdateTime(1500)
		require.Equal(t, 1, *n)
		et.UpdateTime(1600)
		et.UpdateTime(1999)
		require.Equal(t, 1, *n)
		et.UpdateTime(2000)
		require.Equal(t, 2, *n)
	})

	t.Run("initial duration before first reset", func(t *testing.T) {
		et, n := newTimers(500)

		et.UpdateTime(100)
		require.Equal(t, 1, *n)
		et.UpdateTime(599)
		require.Equal(t, 1, *n)
		et.UpdateTime(600)
		require.Equal(t, 2, *n)
	})

	t.Run("reset reschedules", func(t *testing.T) {
		et, n := newTimers(0)
		et.Reset(1000, 500)

		et.UpdateTime(1500)
		require.Equal(t, 1, *n)
		et.Reset(1700, 500)
		et.UpdateTime(2000)
		require.Equal(t, 1, *n)
		et.UpdateTime(2200)
		require.Equal(t, 2, *n)
	})
}
