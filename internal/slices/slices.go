package slices

import "slices"

// TwoDimSliceElementCount returns sum len for ss.
func TwoDimSliceElementCount[E any](s [][]E) int {
	var n int
	for i := range s {
		n += len(s[i])
	}
	return n
}

// NilTwoDimSliceElements returns clone of ss with nil-ed given indexes.
func NilTwoDimSliceElements[T any](s [][]T, idxs []int) [][]T {
	if s == nil {
		return nil
	}

	c := make([][]T, len(s))
	for i := range c {
		if !slices.Contains(idxs, i) {
			c[i] = slices.Clone(s[i])
		}
	}
	return c
}

// CountNilsInTwoDimSlice counts nil elements of s.
func CountNilsInTwoDimSlice[T any](s [][]T) int {
	var n int
	for i := range s {
		if s[i] == nil {
			n++
		}
	}
	return n
}

// AllZeros checks whether s contains all zeros. Returns true if s has no
// elements.
func AllZeros(s []byte) bool {
	for i := range s {
		if s[i] != 0 {
			return false
		}
	}
	return true
}

// RepeatElement returns slice of n shallow copies of e.
func RepeatElement[E any, S []E](n int, e E) S {
	s := make(S, n)
	for i := range s {
		s[i] = e
	}
	return s
}

// MaxLen returns max length in s. Returns 0 if s is empty.
func MaxLen(s []string) int {
	if len(s) == 0 {
		return 0
	}
	mx := len(s[0])
	for i := 1; i < len(s); i++ {
		if len(s[i]) > mx {
			mx = len(s[i])
		}
	}
	return mx
}
