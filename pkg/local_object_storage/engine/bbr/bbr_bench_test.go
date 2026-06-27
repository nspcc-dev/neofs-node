package bbr_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine/bbr"
)

func BenchmarkExps(b *testing.B) {
	loadLimiter := bbr.NewLoadLimiter()

	for b.Loop() {
		_, _ = loadLimiter.Write(64<<20, func() error { return nil })
	}
}
