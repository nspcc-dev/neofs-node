package util

// SliceToMap converts any slice with comparable elements to a map.
func SliceToMap[V comparable](s []V) map[V]struct{} {
	res := make(map[V]struct{}, len(s))
	for _, v := range s {
		res[v] = struct{}{}
	}

	return res
}
