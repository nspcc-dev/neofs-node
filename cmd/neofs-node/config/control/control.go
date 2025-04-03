package controlconfig

import "github.com/nspcc-dev/neo-go/pkg/crypto/keys"

// GRPCEndpointDefault is a default endpoint of gRPC Control service.
const GRPCEndpointDefault = ""

// Control contains configuration for Control service.
type Control struct {
	AuthorizedKeys keys.PublicKeys `mapstructure:"authorized_keys"`

	GRPC struct {
		Endpoint string `mapstructure:"endpoint"`
	} `mapstructure:"grpc"`
}

// Normalize sets default values for Control configuration.
func (c *Control) Normalize() {
	if c.GRPC.Endpoint == "" {
		c.GRPC.Endpoint = GRPCEndpointDefault
	}
}
