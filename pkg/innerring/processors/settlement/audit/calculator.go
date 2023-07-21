package audit

import (
	"go.uber.org/zap"
)

// Calculator represents a component for calculating payments
// based on data audit results and sending remittances to the chain.
type Calculator struct {
	prm *CalculatorPrm

	opts *options
}

// CalculatorOption is a Calculator constructor's option.
type CalculatorOption func(*options)

type options struct {
	log *zap.Logger
}

func defaultOptions() *options {
	return &options{
		log: zap.L(),
	}
}

// NewCalculator creates, initializes and returns a new Calculator instance.
func NewCalculator(p *CalculatorPrm, opts ...CalculatorOption) *Calculator {
	o := defaultOptions()

	for i := range opts {
		opts[i](o)
	}

	return &Calculator{
		prm:  p,
		opts: o,
	}
}

// WithLogger returns an option to specify the logging component.
func WithLogger(l *zap.Logger) CalculatorOption {
	return func(o *options) {
		o.log = l
	}
}
