package controlconfig

import "github.com/nspcc-dev/neo-go/pkg/crypto/keys"

// Control contains configuration for Control service.
type Control struct {
	AuthorizedKeys keys.PublicKeys `mapstructure:"authorized_keys"`

	GRPC struct {
		Endpoint string `mapstructure:"endpoint"`
	} `mapstructure:"grpc"`
}
