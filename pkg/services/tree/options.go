package tree

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"go.uber.org/zap"
)

type cfg struct {
	log       *zap.Logger
	key       *ecdsa.PrivateKey
	nmSource  netmap.Source
	cnrSource container.Source
	forest    pilorama.Forest
}

// Option represents configuration option for a tree service.
type Option func(*cfg)

// WithContainerSource sets a container source for a tree service.
// This option is required.
func WithContainerSource(src container.Source) Option {
	return func(c *cfg) {
		c.cnrSource = src
	}
}

// WithNetmapSource sets a netmap source for a tree service.
// This option is required.
func WithNetmapSource(src netmap.Source) Option {
	return func(c *cfg) {
		c.nmSource = src
	}
}

// WithPrivateKey sets a netmap source for a tree service.
// This option is required.
func WithPrivateKey(key *ecdsa.PrivateKey) Option {
	return func(c *cfg) {
		c.key = key
	}
}

// WithLogger sets logger for a tree service.
func WithLogger(log *zap.Logger) Option {
	return func(c *cfg) {
		c.log = log
	}
}

// WithStorage sets tree storage for a service.
func WithStorage(s pilorama.Forest) Option {
	return func(c *cfg) {
		c.forest = s
	}
}
