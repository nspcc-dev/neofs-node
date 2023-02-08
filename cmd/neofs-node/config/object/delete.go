package objectconfig

import "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"

const (
	deleteSubsection = "delete"

	// DefaultTombstoneLifetime is the default value of tombstone lifetime in epochs.
	DefaultTombstoneLifetime = 5
)

// TombstoneLifetime returns the value of `tombstone_lifetime` config parameter.
func TombstoneLifetime(c *config.Config) uint64 {
	ts := config.UintSafe(c.Sub(subsection).Sub(deleteSubsection), "tombstone_lifetime")
	if ts <= 0 {
		return DefaultTombstoneLifetime
	}
	return ts
}
