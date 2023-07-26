package trustcontroller

import (
	"go.uber.org/zap"
)

// Option sets an optional parameter of Controller.
type Option func(*options)

type options struct {
	log *zap.Logger
}

func defaultOpts() *options {
	return &options{
		log: zap.L(),
	}
}

// WithLogger returns option to specify logging component.
//
// Ignores nil values.
func WithLogger(l *zap.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.log = l
		}
	}
}
