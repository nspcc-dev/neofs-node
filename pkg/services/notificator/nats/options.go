package nats

import (
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func WithClientCert(certPath, keyPath string) Option {
	return func(o *opts) {
		o.nOpts = append(o.nOpts, nats.ClientCert(certPath, keyPath))
	}
}

func WithRootCA(paths ...string) Option {
	return func(o *opts) {
		o.nOpts = append(o.nOpts, nats.RootCAs(paths...))
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(o *opts) {
		o.nOpts = append(o.nOpts, nats.Timeout(timeout))
	}
}

func WithConnectionName(name string) Option {
	return func(o *opts) {
		o.nOpts = append(o.nOpts, nats.Name(name))
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(o *opts) {
		o.logger = logger
	}
}
