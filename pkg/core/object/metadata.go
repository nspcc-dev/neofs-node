package object

import (
	"bytes"
	"math/big"
	"slices"
	"strings"

	"github.com/nspcc-dev/neofs-sdk-go/client"
)

// TODO: docs.
func MergeSearchResults(lim uint16, withAttr bool, sets [][]client.SearchResultItem, mores []bool) ([]client.SearchResultItem, bool) {
	if lim == 0 || len(sets) == 0 {
		return nil, false
	}
	if len(sets) == 1 {
		n := min(uint16(len(sets[0])), lim)
		return sets[0][:n], n < lim || slices.Contains(mores, true)
	}
	lim = calcMaxUniqueSearchResults(lim, sets)
	res := make([]client.SearchResultItem, 0, lim)
	var more bool
	var minInt *big.Int
	for minInd := -1; ; minInd, minInt = -1, nil {
		for i := range sets {
			if len(sets[i]) == 0 {
				continue
			}
			if minInd < 0 {
				minInd = i
				if withAttr {
					minInt, _ = new(big.Int).SetString(sets[i][0].Attributes[0], 10)
				}
				continue
			}
			cmpID := bytes.Compare(sets[i][0].ID[:], sets[minInd][0].ID[:])
			if cmpID == 0 {
				continue
			}
			if withAttr {
				var curInt *big.Int
				if minInt != nil {
					curInt, _ = new(big.Int).SetString(sets[i][0].Attributes[0], 10)
				}
				var cmpAttr int
				if curInt != nil {
					cmpAttr = curInt.Cmp(minInt)
				} else {
					cmpAttr = strings.Compare(sets[i][0].Attributes[0], sets[minInd][0].Attributes[0])
				}
				if cmpAttr != 0 {
					if cmpAttr < 0 {
						minInd = i
						if minInt != nil {
							minInt = curInt
						} else {
							minInt, _ = new(big.Int).SetString(sets[i][0].Attributes[0], 10)
						}
					}
					continue
				}
			}
			if cmpID < 0 {
				minInd = i
				if withAttr {
					minInt, _ = new(big.Int).SetString(sets[minInd][0].Attributes[0], 10)
				}
			}
		}
		if minInd < 0 {
			break
		}
		res = append(res, sets[minInd][0])
		if uint16(len(res)) == lim {
			if more = len(sets[minInd]) > 1 || slices.Contains(mores, true); !more {
			loop:
				for i := range sets {
					if i == minInd {
						continue
					}
					for j := range sets[i] {
						if more = sets[i][j].ID != sets[minInd][0].ID; more {
							break loop
						}
					}
				}
			}
			break
		}
		for i := range sets {
			if i == minInd {
				continue
			}
			for j := range sets[i] {
				if sets[i][j].ID == sets[minInd][0].ID {
					sets[i] = sets[i][j+1:]
					break
				}
			}
		}
		sets[minInd] = sets[minInd][1:]
	}
	return res, more
}

func calcMaxUniqueSearchResults(lim uint16, sets [][]client.SearchResultItem) uint16 {
	n := uint16(len(sets[0]))
	if n >= lim {
		return lim
	}
	for i := 1; i < len(sets); i++ {
	nextItem:
		for j := range sets[i] {
			for k := range i {
				for l := range sets[k] {
					if sets[k][l].ID == sets[i][j].ID {
						continue nextItem
					}
				}
			}
			if n++; n == lim {
				return n
			}
		}
	}
	return n
}
