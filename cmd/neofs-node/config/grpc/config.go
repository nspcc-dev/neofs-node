package grpcconfig

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection    = "grpc"
	tlsSubsection = "tls"
)

var (
	errEndpointNotSet = errors.New("empty/not set endpoint, see `grpc.endpoint` section")
	errTLSKeyNotSet   = errors.New("empty/not set TLS key file path, see `grpc.tls.key` section")
	errTLSCertNotSet  = errors.New("empty/not set TLS certificate file path, see `grpc.tls.certificate` section")
)

// TLSConfig is a wrapper over "tls" config section which provides access
// to TLS configuration of gRPC connection.
type TLSConfig struct {
	cfg *config.Config
}

// Endpoint returns value of "endpoint" config parameter
// from "grpc" section.
//
// Panics if value is not a non-empty string.
func Endpoint(c *config.Config) string {
	v := config.StringSafe(c.Sub(subsection), "endpoint")
	if v == "" {
		panic(errEndpointNotSet)
	}

	return v
}

// TLS returns structure that provides access to "tls" subsection of
// "grpc" section.
func TLS(c *config.Config) TLSConfig {
	return TLSConfig{
		cfg: c.Sub(subsection).Sub(tlsSubsection),
	}
}

// Enabled returns value of "enabled" config parameter.
//
// Returns false if value is not set.
func (tls TLSConfig) Enabled() bool {
	return config.BoolSafe(tls.cfg, "enabled")
}

// KeyFile returns value of "key" config parameter.
//
// Panics if value is not a non-empty string.
func (tls TLSConfig) KeyFile() string {
	v := config.StringSafe(tls.cfg, "key")
	if v == "" {
		panic(errTLSKeyNotSet)
	}

	return v
}

// CertificateFile returns value of "certificate" config parameter.
//
// Panics if value is not a non-empty string.
func (tls TLSConfig) CertificateFile() string {
	v := config.StringSafe(tls.cfg, "certificate")
	if v == "" {
		panic(errTLSCertNotSet)
	}

	return v
}
