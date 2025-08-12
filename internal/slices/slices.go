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
