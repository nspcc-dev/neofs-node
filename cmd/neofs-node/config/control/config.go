package controlconfig

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
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

// AuthorizedKeys parses and returns an array of "authorized_keys" config
// parameter from "control" section.
//
// Returns an empty list if not set.
func AuthorizedKeys(c *config.Config) keys.PublicKeys {
	strKeys := config.StringSliceSafe(c.Sub(subsection), "authorized_keys")
	pubs := make(keys.PublicKeys, 0, len(strKeys))

	for i := range strKeys {
		pub, err := keys.NewPublicKeyFromString(strKeys[i])
		if err != nil {
			panic(fmt.Errorf("invalid permitted key for Control service %s: %w", strKeys[i], err))
		}

		pubs = append(pubs, pub)
	}

	return pubs
}

// GRPC returns a structure that provides access to "grpc" subsection of
// "control" section.
func GRPC(c *config.Config) GRPCConfig {
	return GRPCConfig{
		c.Sub(subsection).Sub(grpcSubsection),
	}
}

// Endpoint returns the value of "endpoint" config parameter.
//
// Returns GRPCEndpointDefault if the value is not a non-empty string.
func (g GRPCConfig) Endpoint() string {
	v := config.String(g.cfg, "endpoint")
	if v != "" {
		return v
	}

	return GRPCEndpointDefault
}
