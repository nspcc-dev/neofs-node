package morph

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

// initialAlphabetNEOAmount represents total amount of GAS distributed between alphabet nodes.
const initialAlphabetNEOAmount = native.NEOTotalSupply

func (c *initializeContext) registerCandidates() error {
	neoHash, err := c.Client.GetNativeContractHash(nativenames.Neo)
	if err != nil {
		return err
	}

	res, err := c.Client.InvokeFunction(neoHash, "getCandidates", []smartcontract.Parameter{}, nil)
	if err != nil {
		return err
	}
	if res.State == vm.HaltState.String() && len(res.Stack) > 0 {
		arr, ok := res.Stack[0].Value().([]stackitem.Item)
		if ok && len(arr) > 0 {
			return nil
		}
	}

	regPrice, err := c.Client.GetCandidateRegisterPrice()
	if err != nil {
		return fmt.Errorf("can't fetch registration price: %w", err)
	}

	sysGas := regPrice + native.GASFactor // + 1 GAS
	for _, w := range c.Wallets {
		acc, err := getWalletAccount(w, singleAccountName)
		if err != nil {
			return err
		}

		w := io.NewBufBinWriter()
		emit.AppCall(w.BinWriter, neoHash, "registerCandidate", callflag.States, acc.PrivateKey().PublicKey().Bytes())
		emit.Opcodes(w.BinWriter, opcode.ASSERT)

		h, err := c.Client.SignAndPushInvocationTx(w.Bytes(), acc, sysGas, 0, []client.SignerAccount{{
			Signer: transaction.Signer{
				Account: acc.Contract.ScriptHash(),
				Scopes:  transaction.CalledByEntry,
			},
			Account: acc,
		}})
		if err != nil {
			return err
		}

		c.Hashes = append(c.Hashes, h)
	}

	return c.awaitTx()
}

func (c *initializeContext) transferNEOToAlphabetContracts() error {
	neoHash, err := c.Client.GetNativeContractHash(nativenames.Neo)
	if err != nil {
		return err
	}

	ok, err := c.transferNEOFinished(neoHash)
	if ok || err != nil {
		return err
	}

	cs, err := c.readContract(alphabetContract)
	if err != nil {
		return fmt.Errorf("can't read alphabet contract: %w", err)
	}

	amount := initialAlphabetNEOAmount / len(c.Wallets)

	bw := io.NewBufBinWriter()
	for _, w := range c.Wallets {
		acc, err := getWalletAccount(w, singleAccountName)
		if err != nil {
			return err
		}

		h := state.CreateContractHash(acc.Contract.ScriptHash(), cs.NEF.Checksum, cs.Manifest.Name)
		emit.AppCall(bw.BinWriter, neoHash, "transfer", callflag.All,
			c.CommitteeAcc.Contract.ScriptHash(), h, int64(amount), nil)
	}

	if err := c.sendCommitteeTx(bw.Bytes(), -1); err != nil {
		return err
	}

	return c.awaitTx()
}

func (c *initializeContext) transferNEOFinished(neoHash util.Uint160) (bool, error) {
	bal, err := c.Client.NEP17BalanceOf(neoHash, c.CommitteeAcc.Contract.ScriptHash())
	return bal < native.NEOTotalSupply, err
}
