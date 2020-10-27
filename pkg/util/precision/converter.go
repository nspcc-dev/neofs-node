package precision

import (
	"math"
	"math/big"
)

type (
	// Converter is cached wrapper on `convert` function. It caches base and
	// target precisions and factor.
	converter struct {
		base   uint32 // base precision
		target uint32 // target precision

		factor *big.Int
	}

	// Fixed8Converter is a converter with base precision of Fixed8. It uses
	// int64 values because there is a guarantee that balance contract will
	// operate with `Deposit` and `Withdraw` amounts that less than 2**53-1.
	// This is a JSON bound that uses neo node. Neo-go has int64 limit for
	// `smartcontract.Parameter` of integer type.
	Fixed8Converter struct {
		converter
	}
)

const fixed8Precision = 8

// convert is the function that converts `n` to desired precision by using
// factor value.
func convert(n, factor *big.Int, decreasePrecision bool) *big.Int {
	if decreasePrecision {
		return new(big.Int).Div(n, factor)
	}

	return new(big.Int).Mul(n, factor)
}

// NewConverter returns Fixed8Converter.
func NewConverter(precision uint32) Fixed8Converter {
	var c Fixed8Converter

	c.SetBalancePrecision(precision)

	return c
}

func (c converter) toTarget(n *big.Int) *big.Int {
	return convert(n, c.factor, c.base > c.target)
}

func (c converter) toBase(n *big.Int) *big.Int {
	return convert(n, c.factor, c.base < c.target)
}

// ToFixed8 converts n of balance contract precision to Fixed8 precision.
func (c Fixed8Converter) ToFixed8(n int64) int64 {
	return c.toBase(new(big.Int).SetInt64(n)).Int64()
}

// ToBalancePrecision converts n of Fixed8 precision to balance contract precision.
func (c Fixed8Converter) ToBalancePrecision(n int64) int64 {
	return c.toTarget(new(big.Int).SetInt64(n)).Int64()
}

// SetBalancePrecision prepares converter to work.
func (c *Fixed8Converter) SetBalancePrecision(precision uint32) {
	exp := int(precision) - fixed8Precision
	if exp < 0 {
		exp = -exp
	}

	c.base = fixed8Precision
	c.target = precision
	c.factor = new(big.Int).SetInt64(int64(math.Pow10(exp)))
}

// Convert is a wrapper of convert function. Use cached `converter` struct
// if fromPrecision and toPrecision are constant.
func Convert(fromPrecision, toPrecision uint32, n *big.Int) *big.Int {
	var decreasePrecision bool

	exp := int(toPrecision) - int(fromPrecision)
	if exp < 0 {
		decreasePrecision = true
		exp = -exp
	}

	factor := new(big.Int).SetInt64(int64(math.Pow10(exp)))

	return convert(n, factor, decreasePrecision)
}
