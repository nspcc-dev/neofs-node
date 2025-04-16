package metabaseconfig

import (
	"io/fs"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/internal"
)

// PermDefault is the default permission bits for metabase file.
const PermDefault = 0o640

// Metabase contains configuration for a metabase.
type Metabase struct {
	Path          string        `mapstructure:"path"`
	Perm          fs.FileMode   `mapstructure:"perm"`
	MaxBatchSize  internal.Size `mapstructure:"max_batch_size"`
	MaxBatchDelay time.Duration `mapstructure:"max_batch_delay"`
}

// Normalize metabase configuration by filling in default values.
func (m *Metabase) Normalize(def Metabase) {
	if m.Perm == 0 {
		if def.Perm == 0 {
			m.Perm = PermDefault
		} else {
			m.Perm = def.Perm
		}
	}
	m.MaxBatchSize.Check(def.MaxBatchSize, 0)
	if m.MaxBatchDelay <= 0 && def.MaxBatchDelay > 0 {
		m.MaxBatchDelay = def.MaxBatchDelay
	}
}
