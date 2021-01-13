package control

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
)

// Server is an entity that serves
// Control service on storage node.
type Server struct {
	*cfg
}

// HealthChecker is component interface for calculating
// the current health status of a node.
type HealthChecker interface {
	// Must calculate and return current node health status.
	//
	// If status can not be calculated for any reason,
	// control.HealthStatus_STATUS_UNDEFINED should be returned.
	HealthStatus() control.HealthStatus
}

// Option of the Server's constructor.
type Option func(*cfg)

type cfg struct {
	key *ecdsa.PrivateKey

	allowedKeys [][]byte

	healthChecker HealthChecker
}

func defaultCfg() *cfg {
	return &cfg{}
}

// New creates, initializes and returns new Server instance.
func New(opts ...Option) *Server {
	c := defaultCfg()

	for _, opt := range opts {
		opt(c)
	}

	return &Server{
		cfg: c,
	}
}

// WithKey returns option to set private key
// used for signing responses.
func WithKey(key *ecdsa.PrivateKey) Option {
	return func(c *cfg) {
		c.key = key
	}
}

// WithAllowedKeys returns option to add list of public
// keys that have rights to use Control service.
func WithAllowedKeys(keys [][]byte) Option {
	return func(c *cfg) {
		c.allowedKeys = append(c.allowedKeys, keys...)
	}
}

// WithHealthChecker returns option to set component
// to calculate node health status.
func WithHealthChecker(hc HealthChecker) Option {
	return func(c *cfg) {
		c.healthChecker = hc
	}
}
