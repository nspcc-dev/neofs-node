package objectconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// PutConfig is a wrapper over "put" config section which provides access
// to object put pipeline configuration of object service.
type PutConfig struct {
	cfg *config.Config
}

const (
	subsection = "object"

	putSubsection = "put"

	// PutPoolSizeDefault is a default value of routine pool size to
	// process object.Put requests in object service.
	PutPoolSizeDefault = 10
)

// Put returns structure that provides access to "put" subsection of
// "object" section.
func Put(c *config.Config) PutConfig {
	return PutConfig{
		c.Sub(subsection).Sub(putSubsection),
	}
}

// PoolSizeRemote returns the value of "pool_size_remote" config parameter.
//
// Returns PutPoolSizeDefault if the value is not a positive number.
func (g PutConfig) PoolSizeRemote() int {
	v := config.Int(g.cfg, "pool_size_remote")
	if v > 0 {
		return int(v)
	}

	return PutPoolSizeDefault
}
