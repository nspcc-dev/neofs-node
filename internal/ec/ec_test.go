package ec_test

import (
	"bytes"
	"fmt"
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
	ln := uint64(len(data))

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

func testConcatDataParts(t *testing.T, ln uint64) {
	data := testutil.RandByteSlice(ln)

	for _, rule := range []iec.Rule{
		{DataPartNum: 3, ParityPartNum: 1},
		{DataPartNum: 12, ParityPartNum: 4},
	} {
		t.Run(fmt.Sprintf("rule=%s", rule.String()), func(t *testing.T) {
			var parts [][]byte
			if ln > 0 {
				rs, err := reedsolomon.New(int(rule.DataPartNum), int(rule.ParityPartNum))
				require.NoError(t, err)

				parts, err = rs.Split(data)
				require.NoError(t, err)
			} else {
				parts = make([][]byte, rule.DataPartNum+rule.ParityPartNum)
			}

			got := iec.ConcatDataParts(rule, ln, parts)
			require.True(t, bytes.Equal(data, got))
		})
	}
}

func TestConcatDataParts(t *testing.T) {
	for _, ln := range []uint64{
		0,
		1,
		100,
		1 << 10,
		1<<10 + 1,
		10 << 10,
		10<<10 + 1,
	} {
		t.Run(fmt.Sprintf("len=%d", ln), func(t *testing.T) {
			testConcatDataParts(t, ln)
		})
	}
}
