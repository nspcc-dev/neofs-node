package gas_test

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/neotest"
	"github.com/nspcc-dev/neo-go/pkg/neotest/chain"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/core/metachain"
)

func newGasClient(t *testing.T) (*neotest.ContractInvoker, *neotest.ContractInvoker) {
	ch, validators, committee := chain.NewMultiWithOptions(t, &chain.Options{
		NewNatives: metachain.NewCustomNatives,
	})
	e := neotest.NewExecutor(t, ch, validators, committee)

	return e.ValidatorInvoker(e.NativeHash(t, nativenames.Gas)), e.CommitteeInvoker(e.NativeHash(t, nativenames.Gas))
}

const defaultBalance = 100 * native.GASFactor

func TestGAS(t *testing.T) {
	gasValidatorsI, gasCommitteeI := newGasClient(t)
	hardcodedBalance := stackitem.Make(defaultBalance)

	t.Run("committee balance", func(t *testing.T) {
		gasCommitteeI.Invoke(t, hardcodedBalance, "balanceOf", gasCommitteeI.Hash)
	})

	t.Run("new account balance", func(t *testing.T) {
		s := gasValidatorsI.NewAccount(t, defaultBalance+1)
		gasCommitteeI.WithSigners(s).Invoke(t, hardcodedBalance, "balanceOf", s.ScriptHash())
	})

	t.Run("transfer does not change balance", func(t *testing.T) {
		newAcc := gasValidatorsI.NewAccount(t, defaultBalance+1)
		gasCommitteeI.Invoke(t, stackitem.Make(true), "transfer", gasCommitteeI.Hash, newAcc.ScriptHash(), 1, stackitem.Null{})
		gasCommitteeI.Invoke(t, hardcodedBalance, "balanceOf", newAcc.ScriptHash())
	})
}
