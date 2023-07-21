package tree

import (
	"crypto/ecdsa"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.uber.org/zap"
)

type ContainerSource interface {
	container.Source
	// List must return list of all the containers in the NeoFS network
	// at the moment of a call and any error that does not allow fetching
	// container information.
	List() ([]cid.ID, error)
}

type cfg struct {
	log        *zap.Logger
	key        *ecdsa.PrivateKey
	rawPub     []byte
	nmSource   netmap.Source
	cnrSource  ContainerSource
	eaclSource container.EACLSource
	forest     pilorama.Forest
	// replication-related parameters
	replicatorChannelCapacity int
	replicatorWorkerCount     int
	replicatorTimeout         time.Duration
	containerCacheSize        int
}

// Option represents configuration option for a tree service.
type Option func(*cfg)

// WithContainerSource sets a container source for a tree service.
// This option is required.
func WithContainerSource(src ContainerSource) Option {
	return func(c *cfg) {
		c.cnrSource = src
	}
}

// WithEACLSource sets a eACL table source for a tree service.
// This option is required.
func WithEACLSource(src container.EACLSource) Option {
	return func(c *cfg) {
		c.eaclSource = src
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

func WithReplicationTimeout(t time.Duration) Option {
	return func(c *cfg) {
		if t > 0 {
			c.replicatorTimeout = t
		}
	}
}
