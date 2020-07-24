package invoke

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-crypto"
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
	extraFee = 5000_0000 // 0.5 Fixed8 gas

	checkIsInnerRingMethod = "IsInnerRing"
	chequeMethod           = "Cheque"
)

// IsInnerRing returns true if 'key' is presented in inner ring list.
func IsInnerRing(cli *client.Client, con util.Uint160, key *ecdsa.PublicKey) (bool, error) {
	if cli == nil {
		return false, client.ErrNilClient
	}

	pubKey := crypto.MarshalPublicKey(key)

	val, err := cli.TestInvoke(con, checkIsInnerRingMethod, pubKey)
	if err != nil {
		return false, err
	}

	isInnerRing, err := client.BoolFromStackParameter(val[0])
	if err != nil {
		return false, err
	}

	return isInnerRing, nil
}

// CashOutCheque invokes Cheque method.
func CashOutCheque(cli *client.Client, con util.Uint160, p *ChequeParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.Invoke(con, extraFee, chequeMethod,
		p.ID,
		p.User.BytesBE(),
		p.Amount,
		p.LockAccount.BytesBE(),
	)
}
