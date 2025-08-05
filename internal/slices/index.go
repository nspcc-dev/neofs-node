package slices

import combinations "github.com/mxschmitt/golang-combinations"

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
