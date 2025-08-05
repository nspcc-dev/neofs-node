package ec

import "iter"

// NodeSequenceForPart returns sorted sequence of node indexes to store object
// for EC part with index = partIdx produced by applying the rule such that
// [Rule.DataPartNum] + [Rule.ParityPartNum] = totalParts.
func NodeSequenceForPart(partIdx, totalParts, nodes int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for shift := 0; shift <= totalParts-1; shift++ {
			for i := (partIdx + shift) % totalParts; i < nodes; i += totalParts {
				if !yield(i) {
					return
				}
			}
		}
	}
}
