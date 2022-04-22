package treeconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// GRPCConfig is a wrapper over "grpc" config section which provides access
// to gRPC configuration of control service.
type GRPCConfig struct {
	cfg *config.Config
}

const (
	subsection     = "tree"
	grpcSubsection = "grpc"

	// GRPCEndpointDefault is a default endpoint of gRPC Control service.
	GRPCEndpointDefault = ""
)

// GRPC returns structure that provides access to "grpc" subsection of
// "tree" section.
func GRPC(c *config.Config) GRPCConfig {
	return GRPCConfig{
		c.Sub(subsection).Sub(grpcSubsection),
	}
}

// Endpoint returns value of "endpoint" config parameter.
//
// Returns GRPCEndpointDefault if value is not a non-empty string.
func (g GRPCConfig) Endpoint() string {
	v := config.String(g.cfg, "endpoint")
	if v != "" {
		return v
	}

	return GRPCEndpointDefault
}
