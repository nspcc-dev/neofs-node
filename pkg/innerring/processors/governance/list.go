package governance

import (
	"errors"
	"fmt"
	"sort"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
)

var (
	errNotEnoughKeys = errors.New("alphabet list in mainnet is too short")
	errNotEqualLen   = errors.New("old and new alphabet lists have different length")
	errEmptyFSChain  = errors.New("FS chain list is empty")
)

// newAlphabetList returns an updated list of FS chain keys with no more than 1\3
// of new keys from the mainnet list. The function returns `errEmptyFSChain` if
// FS chain list is empty. The function returns `errNotEnoughKeys` if the mainnet
// list contains less keys than FS chain list. The function returns (nil, nil) if
// the mainnet list contains all keys from FS chain list.
//
// Sorts passed slices.
func newAlphabetList(fsChain, mainnet keys.PublicKeys) (keys.PublicKeys, error) {
	sort.Sort(fsChain)
	sort.Sort(mainnet)

	ln := len(fsChain)
	if ln == 0 {
		return nil, errEmptyFSChain
	}

	if len(mainnet) < ln {
		return nil, fmt.Errorf("%w: expecting %d keys", errNotEnoughKeys, ln)
	}

	hmap := make(map[string]bool, ln)
	result := make(keys.PublicKeys, 0, ln)

	for _, node := range fsChain {
		hmap[node.Address()] = false
	}

	newNodes := 0
	newNodeLimit := (ln - 1) / 3

	for _, node := range mainnet {
		if len(result) == ln {
			break
		}

		limitReached := newNodes == newNodeLimit

		mainnetAddr := node.Address()
		if _, ok := hmap[mainnetAddr]; !ok {
			if limitReached {
				continue
			}
			newNodes++
		} else {
			hmap[mainnetAddr] = true
		}

		result = append(result, node)
	}

	if newNodes == 0 {
		return nil, nil
	}

	for _, node := range fsChain {
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

// updateInnerRing function removes `before` keys from `innerRing` and adds
// `after` keys in the list. If the length of `before` and `after` is not the same,
// the function returns errNotEqualLen.
func updateInnerRing(innerRing, before, after keys.PublicKeys) (keys.PublicKeys, error) {
	lnBefore := len(before)
	if lnBefore != len(after) {
		return nil, errNotEqualLen
	}

	result := make(keys.PublicKeys, 0, len(innerRing))

	// O(n^2) for 7 nodes is not THAT bad.
loop:
	for i := range innerRing {
		for j := range before {
			if innerRing[i].Equal(before[j]) {
				result = append(result, after[j])
				continue loop
			}
		}
		result = append(result, innerRing[i])
	}

	return result, nil
}
