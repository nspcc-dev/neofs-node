package basic

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

type normalizedValueCase struct {
	name            string
	n, total, limit uint64
	expected        uint64
}

func TestNormalizedValues(t *testing.T) {
	testCases := []normalizedValueCase{
		{
			name:     "zero limit",
			n:        50,
			total:    100,
			limit:    0,
			expected: 0,
		},
		{
			name:     "scale down",
			n:        50,
			total:    100,
			limit:    10,
			expected: 5,
		},
		{
			name:     "scale up",
			n:        50,
			total:    100,
			limit:    1000,
			expected: 500,
		},
	}

	for _, testCase := range testCases {
		testNormalizedValues(t, testCase)
	}
}

func testNormalizedValues(t *testing.T, c normalizedValueCase) {
	n := new(big.Int).SetUint64(c.n)
	total := new(big.Int).SetUint64(c.total)
	limit := new(big.Int).SetUint64(c.limit)
	exp := new(big.Int).SetUint64(c.expected)

	got := normalizedValue(n, total, limit)
	require.Zero(t, exp.Cmp(got), c.name)
}
