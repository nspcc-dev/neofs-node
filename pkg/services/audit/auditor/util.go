package auditor

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
)

// returns random uint64 number [0; n) outside exclude map.
// exclude must contain no more than n-1 elements [0; n)
func nextRandUint64(n uint64, exclude map[uint64]struct{}) uint64 {
	ln := uint64(len(exclude))

	ind := randUint64(n - ln)

	for i := uint64(0); ; i++ {
		if i >= ind {
			if _, ok := exclude[i]; !ok {
				return i
			}
		}
	}
}

// returns random uint64 number [0, n).
func randUint64(n uint64) uint64 {
	return rand.Uint64(rand.New(), int64(n))
}
