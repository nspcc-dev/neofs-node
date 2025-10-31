package pprofconfig

import (
	serviceconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/service"
)

// Pprof configures pprof.
type Pprof struct {
	serviceconfig.Service `mapstructure:",squash"`
	EnableBlock           bool `mapstructure:"enable_block"`
	EnableMutex           bool `mapstructure:"enable_mutex"`
}

// Normalize sets default values for Service fields if they are not set.
func (x *Pprof) Normalize() {
	x.Service.Normalize("localhost:6060")
}
