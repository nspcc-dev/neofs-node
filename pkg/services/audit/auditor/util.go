package auditor

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
)

// nextRandUint64 returns random uint64 number [0; n) outside exclude map.
// Panics if len(exclude) >= n.
func nextRandUint64(n uint64, exclude map[uint64]struct{}) uint64 {
	ln := uint64(len(exclude))
	ind := rand.Uint64() % (n - ln)

	for i := ind; ; i++ {
		if _, ok := exclude[i]; !ok {
			return i
		}
	}
}
