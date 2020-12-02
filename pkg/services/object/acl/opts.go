package acl

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
)

// WithContainerSource returns option to set container source.
func WithContainerSource(v container.Source) Option {
	return func(c *cfg) {
		c.containers = v
	}
}

// WithSenderClassifier returns option to set sender classifier.
func WithSenderClassifier(v SenderClassifier) Option {
	return func(c *cfg) {
		c.sender = v
	}
}

// WithNextService returns option to set next object service.
func WithNextService(v objectSvc.ServiceServer) Option {
	return func(c *cfg) {
		c.next = v
	}
}

// WithEACLValidator returns options to set eACL validator options.
func WithEACLValidatorOptions(v ...eacl.Option) Option {
	return func(c *cfg) {
		c.eACLOpts = v
	}
}

// WithLocalStorage returns options to set local object storage.
func WithLocalStorage(v *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.localStorage = v
	}
}

// WithNetmapState returns options to set global netmap state.
func WithNetmapState(v netmap.State) Option {
	return func(c *cfg) {
		c.state = v
	}
}
