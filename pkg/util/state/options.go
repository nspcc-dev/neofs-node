package state

import (
	"crypto/ecdsa"
	"time"

	"go.uber.org/zap"
)

type cfg struct {
	l          *zap.Logger
	timeout    time.Duration
	privateKey *ecdsa.PrivateKey
}

// Option allows setting optional parameters of the TokenStore.
type Option func(*cfg)

func defaultCfg() *cfg {
	return &cfg{
		l:       zap.L(),
		timeout: time.Second,
	}
}

// WithLogger returns an option to specify
// logger.
func WithLogger(v *zap.Logger) Option {
	return func(c *cfg) {
		c.l = v
	}
}

// WithTimeout returns option to specify
// database connection timeout.
func WithTimeout(v time.Duration) Option {
	return func(c *cfg) {
		c.timeout = v
	}
}

// WithEncryptionKey return an option to encrypt private
// session keys using provided private key.
func WithEncryptionKey(k *ecdsa.PrivateKey) Option {
	return func(c *cfg) {
		c.privateKey = k
	}
}
