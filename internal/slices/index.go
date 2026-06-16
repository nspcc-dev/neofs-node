package slices

import (
	"math/rand/v2"

	combinations "github.com/mxschmitt/golang-combinations"
)

// IndexCombos returns all combinations of n indexes taken k.
func IndexCombos(n, k int) [][]int {
	return combinations.Combinations(Indexes(n), k)
}

// Indexes returns slices filled with n indexes.
func Indexes(n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = i
	}
	return s
}

// CollectIndex returns new slice of shallows copies of s elements with given indexes.
func CollectIndex[E any, S []E](s S, idxs ...int) S {
	newS := make(S, len(idxs))
	for i, idx := range idxs {
		newS[i] = s[idx]
	}
	return newS
}

// ShuffleIndexes returns slice filled with pseudo-randomized index order.
func ShuffleIndexes(n int) []int {
	s := Indexes(n)
	rand.Shuffle(n, func(i, j int) { s[i], s[j] = s[j], s[i] })
	return s
}
