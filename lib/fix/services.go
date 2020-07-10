package fix

import (
	"context"
)

type (
	// Service interface
	Service interface {
		Start(context.Context)
		Stop()
	}

	combiner []Service
)

var _ Service = (combiner)(nil)

// NewServices creates single runner.
func NewServices(items ...Service) Service {
	var svc = make(combiner, 0, len(items))

	for _, item := range items {
		if item == nil {
			continue
		}

		svc = append(svc, item)
	}

	return svc
}

// Start all services.
func (c combiner) Start(ctx context.Context) {
	for _, svc := range c {
		svc.Start(ctx)
	}
}

// Stop all services.
func (c combiner) Stop() {
	for _, svc := range c {
		svc.Stop()
	}
}
