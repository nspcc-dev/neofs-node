package pilorama

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNextTimestamp(t *testing.T) {
	testCases := []struct {
		latest    Timestamp
		pos, size uint64
		expected  Timestamp
	}{
		{0, 0, 1, 1},
		{2, 0, 1, 3},
		{0, 0, 2, 2},
		{0, 1, 2, 1},
		{10, 0, 4, 12},
		{11, 0, 4, 12},
		{12, 0, 4, 16},
		{10, 1, 4, 13},
		{11, 1, 4, 13},
		{12, 1, 4, 13},
		{10, 2, 4, 14},
		{11, 2, 4, 14},
		{12, 2, 4, 14},
		{10, 3, 4, 11},
		{11, 3, 4, 15},
		{12, 3, 4, 15},
	}

	for _, tc := range testCases {
		actual := nextTimestamp(tc.latest, tc.pos, tc.size)
		require.Equal(t, tc.expected, actual,
			"latest %d, pos %d, size %d", tc.latest, tc.pos, tc.size)
	}
}
