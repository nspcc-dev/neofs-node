package policerconfig

import "time"

const (
	// HeadTimeoutDefault is the default object.Head request timeout in policer.
	HeadTimeoutDefault = 5 * time.Second
	// ReplicationCooldownDefault is the default cooldown time b/w replication tasks
	// submitting.
	ReplicationCooldownDefault = 1 * time.Second
	// ObjectBatchSizeDefault is the default replication's objects batch size.
	ObjectBatchSizeDefault = 10
	// MaxWorkersDefault is the default replication's worker pool's maximum size.
	MaxWorkersDefault = 20
)

// Policer contains configuration for the replication policer.
type Policer struct {
	HeadTimeout         time.Duration `mapstructure:"head_timeout"`
	ReplicationCooldown time.Duration `mapstructure:"replication_cooldown"`
	ObjectBatchSize     uint32        `mapstructure:"object_batch_size"`
	MaxWorkers          uint32        `mapstructure:"max_workers"`
}

// Normalize ensures that all fields of Policer have valid values.
// If some of fields are not set or have invalid values, they will be
// set to default values.
func (p *Policer) Normalize() {
	if p.HeadTimeout <= 0 {
		p.HeadTimeout = HeadTimeoutDefault
	}
	if p.ReplicationCooldown <= 0 {
		p.ReplicationCooldown = ReplicationCooldownDefault
	}
	if p.ObjectBatchSize <= 0 {
		p.ObjectBatchSize = ObjectBatchSizeDefault
	}
	if p.MaxWorkers <= 0 {
		p.MaxWorkers = MaxWorkersDefault
	}
}
