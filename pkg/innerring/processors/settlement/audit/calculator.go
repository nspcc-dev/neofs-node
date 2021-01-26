package audit

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// Calculator represents component for calculating payments
// based on data audit results and sending remittances to the chain.
type Calculator struct {
	prm *CalculatorPrm

	opts *options
}

// CalculatorOption is a Calculator constructor's option.
type CalculatorOption func(*options)

type options struct {
	log *logger.Logger
}

func defaultOptions() *options {
	return &options{
		log: zap.L(),
	}
}

// NewCalculator creates, initializes and returns new Calculator instance.
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

// WithLogger returns option to specify logging component.
func WithLogger(l *logger.Logger) CalculatorOption {
	return func(o *options) {
		o.log = l
	}
}
