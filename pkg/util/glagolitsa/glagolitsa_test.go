package glagolitsa_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/util/glagolitsa"
	"github.com/stretchr/testify/require"
)

func TestLetterByIndex(t *testing.T) {
	require.Panics(t, func() { glagolitsa.LetterByIndex(-1) })
	require.Panics(t, func() { glagolitsa.LetterByIndex(glagolitsa.Size) })
	require.Panics(t, func() { glagolitsa.LetterByIndex(glagolitsa.Size + 1) })

	for i, letter := range []string{
		"az",
		"buky",
		"vedi",
		"glagoli",
		"dobro",
		"yest",
		"zhivete",
		"dzelo",
		"zemlja",
		"izhe",
		"izhei",
		"gerv",
		"kako",
		"ljudi",
		"mislete",
		"nash",
		"on",
		"pokoj",
		"rtsi",
		"slovo",
		"tverdo",
		"uk",
		"fert",
		"kher",
		"oht",
		"shta",
		"tsi",
		"cherv",
		"sha",
		"yer",
		"yeri",
		"yerj",
		"yat",
		"jo",
		"yu",
		"small.yus",
		"small.iotated.yus",
		"big.yus",
		"big.iotated.yus",
		"fita",
		"izhitsa",
	} {
		require.Equal(t, letter, glagolitsa.LetterByIndex(i))
	}
}
