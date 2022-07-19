package tree

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"go.uber.org/zap"
)

type cfg struct {
	log       *zap.Logger
	key       *ecdsa.PrivateKey
	rawPub    []byte
	nmSource  netmap.Source
	cnrSource container.Source
	forest    pilorama.Forest
	// replication-related parameters
	replicatorChannelCapacity int
	replicatorWorkerCount     int
	containerCacheSize        int
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
		c.rawPub = (*keys.PublicKey)(&key.PublicKey).Bytes()
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

func WithReplicationChannelCapacity(n int) Option {
	return func(c *cfg) {
		if n > 0 {
			c.replicatorChannelCapacity = n
		}
	}
}

func WithReplicationWorkerCount(n int) Option {
	return func(c *cfg) {
		if n > 0 {
			c.replicatorWorkerCount = n
		}
	}
}

func WithContainerCacheSize(n int) Option {
	return func(c *cfg) {
		if n > 0 {
			c.containerCacheSize = n
		}
	}
}
