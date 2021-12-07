package client

import "github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"

// customFees represents source of customized per-operation fees.
// Can be initialized using var declaration.
//
// Instances are not thread-safe, so they mean initially filling, and then only reading.
type customFees map[string]fixedn.Fixed8

// setFeeForMethod sets fee for the operation executed using specified contract method.
func (x *customFees) setFeeForMethod(method string, fee fixedn.Fixed8) {
	m := *x
	if m == nil {
		m = make(map[string]fixedn.Fixed8, 1)
		*x = m
	}

	m[method] = fee
}

// returns customized for the operation executed using specified contract method.
// Returns false if fee is not customized.
func (x customFees) feeForMethod(method string) (fixedn.Fixed8, bool) {
	v, ok := x[method]
	return v, ok
}

// fees represents source of per-operation fees.
// Can be initialized using var declaration.
//
// Instances are not thread-safe, so they mean initially filling, and then only reading.
type fees struct {
	defaultFee fixedn.Fixed8

	customFees
}

// sets default fee for all operations.
func (x *fees) setDefault(fee fixedn.Fixed8) {
	x.defaultFee = fee
}

// returns fee for the operation executed using specified contract method.
// Returns customized value if it is set. Otherwise, returns default value.
func (x fees) feeForMethod(method string) fixedn.Fixed8 {
	if fee, ok := x.customFees.feeForMethod(method); ok {
		return fee
	}

	return x.defaultFee
}
