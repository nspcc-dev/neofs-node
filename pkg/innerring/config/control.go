package config

import "github.com/nspcc-dev/neo-go/pkg/crypto/keys"

// Control configures control service of IR node.
type Control struct {
	AuthorizedKeys keys.PublicKeys `mapstructure:"authorized_keys"`
	GRPC           GRPC            `mapstructure:"grpc"`
}

// GRPC configures settings of the grpc protocol.
type GRPC struct {
	Endpoint string `mapstructure:"endpoint"`
}
