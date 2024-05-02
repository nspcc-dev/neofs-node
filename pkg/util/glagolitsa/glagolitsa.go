// Package glagolitsa provides Glagolitic script for NeoFS Alphabet.
package glagolitsa

var script = []string{
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
}

const Size = 41

// LetterByIndex returns string representation of Glagolitic letter compatible
// with NeoFS Alphabet contract by index. Index must be in range [0, Size).
//
// Track https://github.com/nspcc-dev/neofs-node/issues/2431
func LetterByIndex(ind int) string {
	return script[ind]
}

// Glagolitsa implement [github.com/nspcc-dev/neofs-contract/deploy.Glagolitsa].
type Glagolitsa struct{}

func (g *Glagolitsa) Size() int {
	return Size
}
func (g *Glagolitsa) LetterByIndex(ind int) string {
	return LetterByIndex(ind)
}
