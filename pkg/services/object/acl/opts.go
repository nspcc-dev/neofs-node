package acl

import (
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
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
func WithNextService(v object.Service) Option {
	return func(c *cfg) {
		c.next = v
	}
}
