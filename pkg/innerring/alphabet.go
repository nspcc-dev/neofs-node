package innerring

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/glagolitsa"
)

type alphabetContracts map[int]util.Uint160

func newAlphabetContracts() alphabetContracts {
	return make(map[int]util.Uint160, glagolitsa.Size)
}

func (a alphabetContracts) GetByIndex(ind int) (util.Uint160, bool) {
	if ind < 0 || ind >= glagolitsa.Size {
		return util.Uint160{}, false
	}

	contract, ok := a[ind]

	return contract, ok
}

func (a alphabetContracts) indexOutOfRange(ind int) bool {
	return ind < 0 && ind >= len(a)
}

func (a alphabetContracts) iterate(f func(int, util.Uint160)) {
	for ind, contract := range a {
		f(ind, contract)
	}
}

func (a *alphabetContracts) set(ind int, h util.Uint160) {
	(*a)[ind] = h
}
