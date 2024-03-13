package grpcconfig

import (
	"errors"
	"strconv"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

var (
	errEndpointNotSet = errors.New("empty/not set endpoint, see `grpc.endpoint` section")
	errTLSKeyNotSet   = errors.New("empty/not set TLS key file path, see `grpc.tls.key` section")
	errTLSCertNotSet  = errors.New("empty/not set TLS certificate file path, see `grpc.tls.certificate` section")
)

// Config is a wrapper over the config section
// which provides access to gRPC server configurations.
type Config config.Config

// Endpoint returns the value of "endpoint" config parameter.
//
// Panics if the value is not a non-empty string.
func (x *Config) Endpoint() string {
	v := config.StringSafe(
		(*config.Config)(x),
		"endpoint")
	if v == "" {
		panic(errEndpointNotSet)
	}

	return v
}

// ConnectionLimit returns the value of "conn_limit" config parameter.
//
// Returns 0 if the value is not a positive integer.
func (x *Config) ConnectionLimit() int {
	v := config.IntSafe(
		(*config.Config)(x),
		"conn_limit")
	if v < 0 {
		return 0
	}

	return int(v)
}

// TLS returns "tls" subsection as a TLSConfig.
//
// Returns nil if "enabled" value of "tls" subsection is false.
func (x *Config) TLS() *TLSConfig {
	sub := (*config.Config)(x).
		Sub("tls")

	if !config.BoolSafe(sub, "enabled") {
		return nil
	}

	return &TLSConfig{
		cfg: sub,
	}
}

// TLSConfig is a wrapper over the config section
// which provides access to TLS configurations
// of the gRPC server.
type TLSConfig struct {
	cfg *config.Config
}

// KeyFile returns the value of "key" config parameter.
//
// Panics if the value is not a non-empty string.
func (tls TLSConfig) KeyFile() string {
	v := config.StringSafe(tls.cfg, "key")
	if v == "" {
		panic(errTLSKeyNotSet)
	}

	return v
}

// CertificateFile returns the value of "certificate" config parameter.
//
// Panics if the value is not a non-empty string.
func (tls TLSConfig) CertificateFile() string {
	v := config.StringSafe(tls.cfg, "certificate")
	if v == "" {
		panic(errTLSCertNotSet)
	}

	return v
}

// IterateEndpoints iterates over subsections of "grpc" section of c,
// wrap them into Config and passes to f.
//
// Section names are expected to be consecutive integer numbers, starting from 0.
//
// Panics if N is not a positive number.
func IterateEndpoints(c *config.Config, f func(*Config)) {
	c = c.Sub("grpc")

	i := uint64(0)
	for ; ; i++ {
		si := strconv.FormatUint(i, 10)

		sc := (*Config)(c.Sub(si))

		e := config.StringSafe((*config.Config)(sc), "endpoint")
		if e == "" {
			break
		}

		f(sc)
	}
	if i == 0 {
		panic("no gRPC server configured")
	}
}
