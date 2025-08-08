package ec

import "iter"

// TODO: worth having the same for PUT.

// TODO: docs.
func NodeSequenceForPartReading(partIdx, parts, nodes int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for shift := 0; shift <= parts-1; shift++ {
			for i := (partIdx + shift) % parts; i < nodes; i += parts {
				if !yield(i) {
					return
				}
			}
		}
	}
}
