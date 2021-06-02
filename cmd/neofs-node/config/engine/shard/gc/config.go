package gcconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// Config is a wrapper over the config section
// which provides access to Shard's GC configurations.
type Config config.Config

// config defaults
const (
	// RemoverBatchSizeDefault is a default batch size for Shard GC's remover.
	RemoverBatchSizeDefault = 100

	// RemoverSleepIntervalDefault is a default sleep interval of Shard GC's remover.
	RemoverSleepIntervalDefault = time.Minute
)

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
}

// RemoverBatchSize returns value of "remover_batch_size"
// config parameter.
//
// Returns RemoverBatchSizeDefault if value is not a positive number.
func (x *Config) RemoverBatchSize() int {
	s := config.IntSafe(
		(*config.Config)(x),
		"remover_batch_size",
	)

	if s > 0 {
		return int(s)
	}

	return RemoverBatchSizeDefault
}

// RemoverSleepInterval returns value of "remover_sleep_interval"
// config parameter.
//
// Returns RemoverSleepIntervalDefault if value is not a positive number.
func (x *Config) RemoverSleepInterval() time.Duration {
	s := config.DurationSafe(
		(*config.Config)(x),
		"remover_sleep_interval",
	)

	if s > 0 {
		return s
	}

	return RemoverSleepIntervalDefault
}
