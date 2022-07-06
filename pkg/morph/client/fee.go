package client

import "github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"

// setFeeForMethod sets fee for the operation executed using specified contract method.
func (x *fees) setFeeForMethod(method string, fee fixedn.Fixed8) {
	if x.customFees == nil {
		x.customFees = make(map[string]fixedn.Fixed8, 1)
	}

	x.customFees[method] = fee
}

// fees represents source of per-operation fees.
// Can be initialized using var declaration.
//
// Instances are not thread-safe, so they mean initially filling, and then only reading.
type fees struct {
	defaultFee fixedn.Fixed8

	// customFees represents source of customized per-operation fees.
	customFees map[string]fixedn.Fixed8
}

// returns fee for the operation executed using specified contract method.
// Returns customized value if it is set. Otherwise, returns default value.
func (x fees) feeForMethod(method string) fixedn.Fixed8 {
	if x.customFees != nil {
		if fee, ok := x.customFees[method]; ok {
			return fee
		}
	}

	return x.defaultFee
}
