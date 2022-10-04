package settlement

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// Option is a Processor constructor's option.
type Option func(*options)

type options struct {
	poolSize int

	log *logger.Logger
}

func defaultOptions() *options {
	const poolSize = 10

	return &options{
		poolSize: poolSize,
		log:      logger.Nop(),
	}
}

// WithLogger returns option to override the component for logging.
func WithLogger(l *logger.Logger) Option {
	return func(o *options) {
		o.log = l
	}
}
