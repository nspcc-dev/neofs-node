package writecacheconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/internal"
)

const (
	// SizeLimitDefault is the default write-cache size limit.
	SizeLimitDefault = 1 << 30
)

// WriteCache contains configuration for write cache.
type WriteCache struct {
	Enabled  *bool         `mapstructure:"enabled"`
	Path     string        `mapstructure:"path"`
	Capacity internal.Size `mapstructure:"capacity"`
	NoSync   *bool         `mapstructure:"no_sync"`
}

// Normalize sets default values for write cache fields if they are not set.
func (wc *WriteCache) Normalize(def WriteCache) {
	wc.Enabled = internal.CheckPtrBool(wc.Enabled, def.Enabled)
	wc.NoSync = internal.CheckPtrBool(wc.NoSync, def.NoSync)
	wc.Capacity.Check(def.Capacity, SizeLimitDefault)
}
