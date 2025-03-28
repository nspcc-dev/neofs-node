package config

import "github.com/nspcc-dev/neo-go/pkg/crypto/keys"

type Control struct {
	AuthorizedKeys keys.PublicKeys `mapstructure:"authorized_keys"`
	GRPC           GRPC            `mapstructure:"grpc"`
}

type GRPC struct {
	Endpoint string `mapstructure:"endpoint"`
}
