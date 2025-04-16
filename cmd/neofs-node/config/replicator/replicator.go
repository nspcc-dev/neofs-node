package replicatorconfig

import "time"

// PutTimeoutDefault is the default timeout of object put request in replicator.
const PutTimeoutDefault = time.Minute

// Replicator contains configuration for replicator.
type Replicator struct {
	PutTimeout time.Duration `mapstructure:"put_timeout"`
	PoolSize   int           `mapstructure:"pool_size"`
}

// Normalize sets default values for Replicator fields if they are not set.
func (r *Replicator) Normalize() {
	if r.PutTimeout <= 0 {
		r.PutTimeout = PutTimeoutDefault
	}
}
