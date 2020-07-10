package implementations

import (
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/lib/blockchain/goclient"
	"github.com/pkg/errors"
)

// MorphBalanceContract is a wrapper over NeoFS Balance contract client
// that provides an interface of manipulations with user funds.
type MorphBalanceContract struct {
	// NeoFS Balance smart-contract
	balanceContract StaticContractClient

	// "balance of" method name of balance contract
	balanceOfMethodName string

	// decimals method name of balance contract
	decimalsMethodName string
}

// BalanceOfParams is a structure that groups the parameters
// for NeoFS user balance receiving operation.
type BalanceOfParams struct {
	owner refs.OwnerID
}

// BalanceOfResult is a structure that groups the values
// of the result of NeoFS user balance receiving operation.
type BalanceOfResult struct {
	amount int64
}

// DecimalsParams is a structure that groups the parameters
// for NeoFS token decimals receiving operation.
type DecimalsParams struct {
}

// DecimalsResult is a structure that groups the values
// of the result of NeoFS token decimals receiving operation.
type DecimalsResult struct {
	dec int64
}

// SetBalanceContractClient is a Balance contract client setter.
func (s *MorphBalanceContract) SetBalanceContractClient(v StaticContractClient) {
	s.balanceContract = v
}

// SetBalanceOfMethodName is a Balance contract balanceOf method name setter.
func (s *MorphBalanceContract) SetBalanceOfMethodName(v string) {
	s.balanceOfMethodName = v
}

// SetDecimalsMethodName is a Balance contract decimals method name setter.
func (s *MorphBalanceContract) SetDecimalsMethodName(v string) {
	s.decimalsMethodName = v
}

// BalanceOf performs the test invocation call of balanceOf method of NeoFS Balance contract.
func (s MorphBalanceContract) BalanceOf(p BalanceOfParams) (*BalanceOfResult, error) {
	owner := p.OwnerID()

	u160, err := address.StringToUint160(owner.String())
	if err != nil {
		return nil, errors.Wrap(err, "could not convert wallet address to Uint160")
	}

	prms, err := s.balanceContract.TestInvoke(
		s.balanceOfMethodName,
		u160.BytesBE(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "could not perform test invocation")
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (balanceOf): %d", ln)
	}

	amount, err := goclient.IntFromStackParameter(prms[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get integer stack item from stack item (amount)")
	}

	res := new(BalanceOfResult)
	res.SetAmount(amount)

	return res, nil
}

// Decimals performs the test invocation call of decimals method of NeoFS Balance contract.
func (s MorphBalanceContract) Decimals(DecimalsParams) (*DecimalsResult, error) {
	prms, err := s.balanceContract.TestInvoke(
		s.decimalsMethodName,
	)
	if err != nil {
		return nil, errors.Wrap(err, "could not perform test invocation")
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (decimals): %d", ln)
	}

	dec, err := goclient.IntFromStackParameter(prms[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get integer stack item from stack item (decimal)")
	}

	res := new(DecimalsResult)
	res.SetDecimals(dec)

	return res, nil
}

// SetOwnerID is an owner ID setter.
func (s *BalanceOfParams) SetOwnerID(v refs.OwnerID) {
	s.owner = v
}

// OwnerID is an owner ID getter.
func (s BalanceOfParams) OwnerID() refs.OwnerID {
	return s.owner
}

// SetAmount is an funds amount setter.
func (s *BalanceOfResult) SetAmount(v int64) {
	s.amount = v
}

// Amount is an funds amount getter.
func (s BalanceOfResult) Amount() int64 {
	return s.amount
}

// SetDecimals is a decimals setter.
func (s *DecimalsResult) SetDecimals(v int64) {
	s.dec = v
}

// Decimals is a decimals getter.
func (s DecimalsResult) Decimals() int64 {
	return s.dec
}
