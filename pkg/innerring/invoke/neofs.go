package invoke

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

type (
	// ChequeParams for CashOutCheque invocation.
	ChequeParams struct {
		ID          []byte
		Amount      int64 // Fixed8
		User        util.Uint160
		LockAccount util.Uint160
	}
)

const (
	// Extra SysFee for contract invocations. Contracts execute inner ring
	// invocations in two steps: collection and execution. At collection
	// step contract waits for (2\3*n + 1) invocations, and then in execution
	// stage contract actually makes changes in the contract storage. SysFee
	// for invocation calculated based on testinvoke which happens at collection
	// stage. Therefore client has to provide some extra SysFee to operate at
	// execution stage. Otherwise invocation will fail due to gas limit.
	extraFee = 2_0000_0000 // 2.0 Fixed8 gas

	chequeMethod         = "cheque"
	alphabetUpdateMethod = "alphabetUpdate"
)

// CashOutCheque invokes Cheque method.
func CashOutCheque(cli *client.Client, con util.Uint160, fee MainFeeProvider, p *ChequeParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	if !cli.NotaryEnabled() {
		return cli.Invoke(con, fee.MainChainFee(), chequeMethod,
			p.ID,
			p.User.BytesBE(),
			p.Amount,
			p.LockAccount.BytesBE(),
		)
	}

	return cli.NotaryInvoke(con, chequeMethod,
		p.ID,
		p.User.BytesBE(),
		p.Amount,
		p.LockAccount.BytesBE(),
	)
}

// AlphabetUpdate invokes alphabetUpdate method.
func AlphabetUpdate(cli *client.Client, con util.Uint160, fee MainFeeProvider, id []byte, list keys.PublicKeys) error {
	if cli == nil {
		return client.ErrNilClient
	}

	if !cli.NotaryEnabled() {
		return cli.Invoke(con, fee.MainChainFee(), alphabetUpdateMethod, id, list)
	}

	return cli.NotaryInvoke(con, alphabetUpdateMethod, id, list)
}
