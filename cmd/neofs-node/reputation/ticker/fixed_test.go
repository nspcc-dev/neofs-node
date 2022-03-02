package ticker

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixedTimer_Tick(t *testing.T) {
	tests := [...]struct {
		duration uint64
		times    uint64
		err      error
	}{
		{
			duration: 20,
			times:    4,
			err:      nil,
		},
		{
			duration: 6,
			times:    6,
			err:      nil,
		},
		{
			duration: 10,
			times:    6,
			err:      nil,
		},
		{
			duration: 5,
			times:    6,
			err:      errors.New("impossible to tick 6 times in 5 blocks"),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("duration:%d,times:%d", test.duration, test.times), func(t *testing.T) {
			counter := uint64(0)

			timer, err := NewIterationsTicker(test.duration, test.times, func() {
				counter++
			})
			if test.err != nil {
				require.EqualError(t, err, test.err.Error())
				return
			}

			require.NoError(t, err)

			for i := 0; i < int(test.duration); i++ {
				if !timer.Tick() {
					break
				}
			}

			require.Equal(t, false, timer.Tick())
			require.Equal(t, test.times, counter)
		})
	}
}

func TestFixedTimer_RareCalls(t *testing.T) {
	tests := [...]struct {
		duration  uint64
		times     uint64
		firstCall uint64
		period    uint64
	}{
		{
			duration:  11,
			times:     6,
			firstCall: 1,
			period:    2,
		},
		{
			duration:  11,
			times:     4,
			firstCall: 2,
			period:    3,
		},
		{
			duration:  20,
			times:     3,
			firstCall: 4,
			period:    7,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("duration:%d,times:%d", test.duration, test.times), func(t *testing.T) {
			var counter uint64

			timer, err := NewIterationsTicker(test.duration, test.times, func() {
				counter++
			})
			require.NoError(t, err)

			checked := false

			for i := 1; i <= int(test.duration); i++ {
				if !timer.Tick() {
					break
				}

				if !checked && counter == 1 {
					require.Equal(t, test.firstCall, uint64(i))
					checked = true
				}
			}

			require.Equal(t, false, timer.Tick())
			require.Equal(t, test.times, counter)
		})
	}
}
