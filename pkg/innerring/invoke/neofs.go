package invoke

import (
	"bytes"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neo-go/pkg/util"
	crypto "github.com/nspcc-dev/neofs-crypto"
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

	innerRingListMethod = "innerRingList"
	chequeMethod        = "cheque"
)

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

// InnerRingIndex returns index of the `key` in the inner ring list from sidechain
// along with total size of inner ring list. If key is not in the inner ring list,
// then returns `-1` as index.
func InnerRingIndex(cli *client.Client, con util.Uint160, key *ecdsa.PublicKey) (int32, int32, error) {
	if cli == nil {
		return 0, 0, client.ErrNilClient
	}

	nodePublicKey := crypto.MarshalPublicKey(key)

	data, err := cli.TestInvoke(con, innerRingListMethod)
	if err != nil {
		return 0, 0, err
	}

	irNodes, err := client.ArrayFromStackItem(data[0])
	if err != nil {
		return 0, 0, err
	}

	var result int32 = -1

	for i := range irNodes {
		key, err := client.ArrayFromStackItem(irNodes[i])
		if err != nil {
			return 0, 0, err
		}

		keyValue, err := client.BytesFromStackItem(key[0])
		if err != nil {
			return 0, 0, err
		}

		if bytes.Equal(keyValue, nodePublicKey) {
			result = int32(i)
			break
		}
	}

	return result, int32(len(irNodes)), nil
}
