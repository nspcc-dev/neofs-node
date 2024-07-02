package fstree

import (
	"math"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/spf13/cast"
)

// Config is a wrapper over the config section
// which provides access to FSTree configurations.
type Config config.Config

const (
	// DepthDefault is the default shallow dir depth.
	DepthDefault = 4
	// CombinedCountLimitDefault is the default for the maximum number of objects to write into a single file.
	CombinedCountLimitDefault = 128
	// CombinedSizeLimitDefault is the default for the maximum size of the combined object file.
	CombinedSizeLimitDefault = 8 * 1024 * 1024
	// CombinedSizeThresholdDefault is the default for the minimal size of the object that won't be combined with others for writes.
	CombinedSizeThresholdDefault = 128 * 1024
)

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
}

// Type returns the storage type.
func (x *Config) Type() string {
	return fstree.Type
}

// Depth returns the value of "depth" config parameter.
//
// Returns DepthDefault if the value is out of
// [1:fstree.MaxDepth] range.
func (x *Config) Depth() uint64 {
	d := config.UintSafe(
		(*config.Config)(x),
		"depth",
	)

	if d >= 1 && d <= fstree.MaxDepth {
		return d
	}

	return DepthDefault
}

// NoSync returns the value of "no_sync" config parameter.
//
// Returns false if the value is not a boolean or is missing.
func (x *Config) NoSync() bool {
	return config.BoolSafe((*config.Config)(x), "no_sync")
}

// CombinedCountLimit returns the value of "combined_count_limit" config parameter.
//
// Returns [CombinedCountLimitDefault] if the value is missing or not a positive integer.
func (x *Config) CombinedCountLimit() int {
	var v = (*config.Config)(x).Value("combined_count_limit")
	if v == nil {
		return CombinedCountLimitDefault
	}

	i, err := cast.ToIntE(v)
	if err != nil {
		return CombinedCountLimitDefault
	}
	return i
}

// CombinedSizeLimit returns the value of "combined_size_limit" config parameter.
//
// Returns [CombinedSizeLimitDefault] if the value is missing, equal to 0 or not a proper size specification.
func (x *Config) CombinedSizeLimit() int {
	var s = config.SizeInBytesSafe((*config.Config)(x), "combined_size_limit")
	if s == 0 || s > math.MaxInt {
		return CombinedSizeLimitDefault
	}
	return int(s)
}

// CombinedSizeThreshold returns the value of "combined_size_threshold" config parameter.
//
// Returns [CombinedSizeThresholdDefault] if the value is missing, equal to 0 or not a proper size specification.
func (x *Config) CombinedSizeThreshold() int {
	var s = config.SizeInBytesSafe((*config.Config)(x), "combined_size_threshold")
	if s == 0 || s > math.MaxInt {
		return CombinedSizeThresholdDefault
	}
	return int(s)
}
