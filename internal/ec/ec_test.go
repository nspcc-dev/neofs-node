package ec_test

import (
	"testing"

	"github.com/klauspost/reedsolomon"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestRule_String(t *testing.T) {
	r := iec.Rule{
		DataPartNum:   12,
		ParityPartNum: 23,
	}
	require.Equal(t, "12/23", r.String())
}

func testEncode(t *testing.T, rule iec.Rule, data []byte) {
	ln := uint(len(data))

	parts, err := iec.Encode(rule, data)
	require.NoError(t, err)

	res, err := iec.Decode(rule, ln, parts)
	require.NoError(t, err)
	require.Equal(t, data, res)

	for lostCount := 1; lostCount <= int(rule.ParityPartNum); lostCount++ {
		for _, lostIdxs := range islices.IndexCombos(len(parts), lostCount) {
			res, err := iec.Decode(rule, ln, islices.NilTwoDimSliceElements(parts, lostIdxs))
			require.NoError(t, err)
			require.Equal(t, data, res)
		}
	}

	for _, lostIdxs := range islices.IndexCombos(len(parts), int(rule.ParityPartNum)+1) {
		_, err := iec.Decode(rule, ln, islices.NilTwoDimSliceElements(parts, lostIdxs))
		require.ErrorContains(t, err, "restore Reed-Solomon")
		require.ErrorIs(t, err, reedsolomon.ErrTooFewShards)
	}
}

func TestEncode(t *testing.T) {
	rules := []iec.Rule{
		{DataPartNum: 3, ParityPartNum: 1},
		{DataPartNum: 12, ParityPartNum: 4},
	}

	data := testutil.RandByteSlice(4 << 10)

	t.Run("empty", func(t *testing.T) {
		for _, rule := range rules {
			t.Run(rule.String(), func(t *testing.T) {
				test := func(t *testing.T, data []byte) {
					res, err := iec.Encode(rule, []byte{})
					require.NoError(t, err)

					total := int(rule.DataPartNum + rule.ParityPartNum)
					require.Len(t, res, total)
					require.EqualValues(t, total, islices.CountNilsInTwoDimSlice(res))
				}
				test(t, nil)
				test(t, []byte{})
			})
		}
	})

	for _, rule := range rules {
		t.Run(rule.String(), func(t *testing.T) {
			testEncode(t, rule, data)
		})
	}
}
