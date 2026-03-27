package metabaseconfig

import (
	"io/fs"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/internal"
)

const (
	// PermDefault is the default permission bits for metabase file.
	PermDefault = 0o640
	// SearchIterationLimitDefault is the default limit for the number of iterations without finding a match in search.
	SearchIterationLimitDefault = 10000
)

// Metabase contains configuration for a metabase.
type Metabase struct {
	Path                 string        `mapstructure:"path"`
	Perm                 fs.FileMode   `mapstructure:"perm"`
	MaxBatchSize         internal.Size `mapstructure:"max_batch_size"`
	MaxBatchDelay        time.Duration `mapstructure:"max_batch_delay"`
	SearchIterationLimit int           `mapstructure:"search_iteration_limit"`
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
	if m.SearchIterationLimit == -1 {
		m.SearchIterationLimit = 0
	} else if m.SearchIterationLimit <= 0 {
		if def.SearchIterationLimit == -1 {
			m.SearchIterationLimit = 0
		} else if def.SearchIterationLimit > 0 {
			m.SearchIterationLimit = def.SearchIterationLimit
		} else {
			m.SearchIterationLimit = SearchIterationLimitDefault
		}
	}
}
