package replicator

import (
	"context"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

// TODO: docs.
type Transport interface {
	ReplicateToNode(ctx context.Context, req []byte, node netmap.NodeInfo) error
}

// Replicator represents the utility that replicates
// local objects to remote nodes.
type Replicator struct {
	*cfg
	signer    neofscrypto.Signer
	transport Transport
}

// Option is an option for Policer constructor.
type Option func(*cfg)

type cfg struct {
	putTimeout time.Duration

	log *zap.Logger

	localStorage *engine.StorageEngine
}

func defaultCfg() *cfg {
	return &cfg{}
}

// New creates, initializes and returns Replicator instance.
func New(signer neofscrypto.Signer, transport Transport, opts ...Option) *Replicator {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	c.log = c.log.With(zap.String("component", "Object Replicator"))

	return &Replicator{
		cfg:       c,
		signer:    signer,
		transport: transport,
	}
}

// WithPutTimeout returns option to set Put timeout of Replicator.
func WithPutTimeout(v time.Duration) Option {
	return func(c *cfg) {
		c.putTimeout = v
	}
}

// WithLogger returns option to set Logger of Replicator.
func WithLogger(v *zap.Logger) Option {
	return func(c *cfg) {
		c.log = v
	}
}

// WithLocalStorage returns option to set local object storage of Replicator.
func WithLocalStorage(v *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.localStorage = v
	}
}
