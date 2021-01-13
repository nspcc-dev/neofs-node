package private

import (
	"crypto/ecdsa"
)

// Server is an entity that serves
// Private service on storage node.
type Server struct {
	*cfg
}

// Option of the Server's constructor.
type Option func(*cfg)

type cfg struct {
	key *ecdsa.PrivateKey

	allowedKeys [][]byte
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
// keys that have rights to use private service.
func WithAllowedKeys(keys [][]byte) Option {
	return func(c *cfg) {
		c.allowedKeys = append(c.allowedKeys, keys...)
	}
}
