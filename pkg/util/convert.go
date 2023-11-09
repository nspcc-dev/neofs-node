package util

// MapToSlice converts the map to a slice. Order is not
// fixed and _is not_ ascending or descending.
func MapToSlice[V comparable](m map[V]struct{}) []V {
	res := make([]V, 0, len(m))
	for v := range m {
		res = append(res, v)
	}

	return res
}
