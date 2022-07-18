package morph

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
)

// initialAlphabetNEOAmount represents the total amount of GAS distributed between alphabet nodes.
const initialAlphabetNEOAmount = native.NEOTotalSupply

func (c *initializeContext) registerCandidates() error {
	neoHash := c.nativeHash(nativenames.Neo)

	res, err := invokeFunction(c.Client, neoHash, "getCandidates", nil, nil)
	if err != nil {
		return err
	}
	if res.State == vmstate.Halt.String() && len(res.Stack) > 0 {
		arr, ok := res.Stack[0].Value().([]stackitem.Item)
		if ok && len(arr) > 0 {
			return nil
		}
	}

	regPrice, err := c.getCandidateRegisterPrice()
	if err != nil {
		return fmt.Errorf("can't fetch registration price: %w", err)
	}

	sysGas := regPrice + native.GASFactor // + 1 GAS
	for _, acc := range c.Accounts {
		w := io.NewBufBinWriter()
		emit.AppCall(w.BinWriter, neoHash, "registerCandidate", callflag.States, acc.PrivateKey().PublicKey().Bytes())
		emit.Opcodes(w.BinWriter, opcode.ASSERT)

		if err := c.sendSingleTx(w.Bytes(), sysGas, acc); err != nil {
			return err
		}
	}

	return c.awaitTx()
}

func (c *initializeContext) transferNEOToAlphabetContracts() error {
	neoHash := c.nativeHash(nativenames.Neo)

	ok, err := c.transferNEOFinished(neoHash)
	if ok || err != nil {
		return err
	}

	cs := c.getContract(alphabetContract)
	amount := initialAlphabetNEOAmount / len(c.Wallets)

	bw := io.NewBufBinWriter()
	for _, acc := range c.Accounts {
		h := state.CreateContractHash(acc.Contract.ScriptHash(), cs.NEF.Checksum, cs.Manifest.Name)
		emit.AppCall(bw.BinWriter, neoHash, "transfer", callflag.All,
			c.CommitteeAcc.Contract.ScriptHash(), h, int64(amount), nil)
		emit.Opcodes(bw.BinWriter, opcode.ASSERT)
	}

	if err := c.sendCommitteeTx(bw.Bytes(), -1, false); err != nil {
		return err
	}

	return c.awaitTx()
}

func (c *initializeContext) transferNEOFinished(neoHash util.Uint160) (bool, error) {
	bal, err := c.Client.NEP17BalanceOf(neoHash, c.CommitteeAcc.Contract.ScriptHash())
	return bal < native.NEOTotalSupply, err
}

var errGetPriceInvalid = errors.New("`getRegisterPrice`: invalid response")

func (c *initializeContext) getCandidateRegisterPrice() (int64, error) {
	switch ct := c.Client.(type) {
	case *client.Client:
		return ct.GetCandidateRegisterPrice()
	default:
		neoHash := c.nativeHash(nativenames.Neo)
		res, err := invokeFunction(c.Client, neoHash, "getRegisterPrice", nil, nil)
		if err != nil {
			return 0, err
		}
		if len(res.Stack) == 0 {
			return 0, errGetPriceInvalid
		}
		bi, err := res.Stack[0].TryInteger()
		if err != nil || !bi.IsInt64() {
			return 0, errGetPriceInvalid
		}
		return bi.Int64(), nil
	}
}
