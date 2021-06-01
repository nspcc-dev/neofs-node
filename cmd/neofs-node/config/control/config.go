package controlconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// GRPCConfig is a wrapper over "grpc" config section which provides access
// to gRPC configuration of control service.
type GRPCConfig struct {
	cfg *config.Config
}

const (
	subsection     = "control"
	grpcSubsection = "grpc"

	// GRPCEndpointDefault is a default endpoint of gRPC Control service.
	GRPCEndpointDefault = ""
)

// AuthorizedKeysString returns string array of "authorized_keys" config
// parameter from "control" section.
//
// Returns empty list if not set.
func AuthorizedKeysString(c *config.Config) []string {
	return config.StringSliceSafe(c.Sub(subsection), "authorized_keys")
}

// GRPC returns structure that provides access to "grpc" subsection of
// "control" section.
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
