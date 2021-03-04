package eacl

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

func WithLogger(v *logger.Logger) Option {
	return func(c *cfg) {
		c.logger = v
	}
}

func WithEACLStorage(v Storage) Option {
	return func(c *cfg) {
		c.storage = v
	}
}
