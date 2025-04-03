package gcconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/internal"
)

const (
	// RemoverBatchSizeDefault is the default batch size for Shard GC's remover.
	RemoverBatchSizeDefault = 100
	// RemoverSleepIntervalDefault is the default sleep interval of Shard GC's remover.
	RemoverSleepIntervalDefault = time.Minute
)

// GC contains configuration for Shard GC.
type GC struct {
	RemoverBatchSize     internal.Size `mapstructure:"remover_batch_size"`
	RemoverSleepInterval time.Duration `mapstructure:"remover_sleep_interval"`
}

// Normalize ensures that all fields of GC have valid values.
// If some of fields are not set or have invalid values, they will be
// set to default values.
func (g *GC) Normalize(def GC) {
	g.RemoverBatchSize.Check(def.RemoverBatchSize, RemoverBatchSizeDefault)
	if g.RemoverSleepInterval <= 0 {
		if def.RemoverSleepInterval <= 0 {
			g.RemoverSleepInterval = RemoverSleepIntervalDefault
		} else {
			g.RemoverSleepInterval = def.RemoverSleepInterval
		}
	}
}
