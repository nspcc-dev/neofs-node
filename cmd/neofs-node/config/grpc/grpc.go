package grpcconfig

// GRPC contains configuration for a single gRPC endpoint.
type GRPC struct {
	Endpoint  string `mapstructure:"endpoint"`
	ConnLimit int    `mapstructure:"conn_limit"`

	TLS TLS `mapstructure:"tls"`
}

// TLS contains configuration for TLS settings.
type TLS struct {
	Enabled     bool   `mapstructure:"enabled"`
	Certificate string `mapstructure:"certificate"`
	Key         string `mapstructure:"key"`
}

// Normalize sets default values for GRPC configuration.
func (g *GRPC) Normalize() {
	if g.ConnLimit < 0 {
		g.ConnLimit = 0
	}
}
