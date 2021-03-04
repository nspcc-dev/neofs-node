package governance

import (
	"sort"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/pkg/errors"
)

var errNotEnoughKeys = errors.New("alphabet list in mainnet is too short")

// newAlphabetList returns updated list of sidechain keys with no more than 1\3
// of new keys from mainnet list. Function returns `errNotEnoughKeys` if
// mainnet list contains less keys than sidechain list. Function returns
// (nil, nil) if mainnet list contains all keys from sidechain list.
func newAlphabetList(sidechain, mainnet keys.PublicKeys) (keys.PublicKeys, error) {
	ln := len(sidechain)
	if len(mainnet) < ln {
		return nil, errors.Wrapf(errNotEnoughKeys, "expecting %d keys", ln)
	}

	hmap := make(map[string]bool, ln)
	result := make(keys.PublicKeys, 0, ln)

	for _, node := range sidechain {
		hmap[node.Address()] = false
	}

	newNodes := 0
	newNodeLimit := (ln - 1) / 3

	for i := 0; i < ln; i++ {
		if newNodes == newNodeLimit {
			break
		}

		mainnetAddr := mainnet[i].Address()
		if _, ok := hmap[mainnetAddr]; !ok {
			newNodes++
		} else {
			hmap[mainnetAddr] = true
		}

		result = append(result, mainnet[i])
	}

	if newNodes == 0 {
		return nil, nil
	}

	for _, node := range sidechain {
		if len(result) == ln {
			break
		}

		if !hmap[node.Address()] {
			result = append(result, node)
		}
	}

	sort.Sort(result)

	return result, nil
}
