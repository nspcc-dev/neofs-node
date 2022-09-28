package nats

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
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

func WithLogger(logger *logger.Logger) Option {
	return func(o *opts) {
		o.log = logger
	}
}
