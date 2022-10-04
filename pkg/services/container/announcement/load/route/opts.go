package loadroute

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// Option sets an optional parameter of Router.
type Option func(*options)

type options struct {
	log *logger.Logger
}

func defaultOpts() *options {
	return &options{
		log: logger.Nop(),
	}
}

// WithLogger returns Option to specify logging component.
func WithLogger(l *logger.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.log = l
		}
	}
}
