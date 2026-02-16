package metaconfig

import "github.com/nspcc-dev/neofs-node/pkg/innerring/config"

// Meta contains configuration for Meta service.
type Meta struct {
	Path string `mapstructure:"path"`

	// Neo RPC service configuration.
	//
	// Optional.
	RPC config.RPC `mapstructure:"rpc"`
}
