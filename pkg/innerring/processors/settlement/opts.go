package settlement

import (
	"go.uber.org/zap"
)

// Option is a Processor constructor's option.
type Option func(*options)

type options struct {
	log *zap.Logger
}

func defaultOptions() *options {
	return &options{
		log: zap.L(),
	}
}

// WithLogger returns option to override the component for logging.
func WithLogger(l *zap.Logger) Option {
	return func(o *options) {
		o.log = l
	}
}
