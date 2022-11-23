package morph

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/neo"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
)

// initialAlphabetNEOAmount represents the total amount of GAS distributed between alphabet nodes.
const initialAlphabetNEOAmount = native.NEOTotalSupply

func (c *initializeContext) registerCandidates() error {
	neoHash := neo.Hash

	cc, err := unwrap.Array(c.ReadOnlyInvoker.Call(neoHash, "getCandidates"))
	if err != nil {
		return fmt.Errorf("`getCandidates`: %w", err)
	}

	if len(cc) > 0 {
		c.Command.Println("Candidates are already registered.")
		return nil
	}

	regPrice, err := c.getCandidateRegisterPrice()
	if err != nil {
		return fmt.Errorf("can't fetch registration price: %w", err)
	}

	w := io.NewBufBinWriter()
	emit.AppCall(w.BinWriter, neoHash, "setRegisterPrice", callflag.States, 1)
	for _, acc := range c.Accounts {
		emit.AppCall(w.BinWriter, neoHash, "registerCandidate", callflag.States, acc.PrivateKey().PublicKey().Bytes())
		emit.Opcodes(w.BinWriter, opcode.ASSERT)
	}
	emit.AppCall(w.BinWriter, neoHash, "setRegisterPrice", callflag.States, regPrice)
	if w.Err != nil {
		panic(fmt.Sprintf("BUG: %v", w.Err))
	}

	signers := []rpcclient.SignerAccount{{
		Signer:  c.getSigner(false, c.CommitteeAcc),
		Account: c.CommitteeAcc,
	}}
	for i := range c.Accounts {
		signers = append(signers, rpcclient.SignerAccount{
			Signer: transaction.Signer{
				Account:          c.Accounts[i].Contract.ScriptHash(),
				Scopes:           transaction.CustomContracts,
				AllowedContracts: []util.Uint160{neoHash},
			},
			Account: c.Accounts[i],
		})
	}

	tx, err := c.Client.CreateTxFromScript(w.Bytes(), c.CommitteeAcc, -1, 0, signers)
	if err != nil {
		return fmt.Errorf("can't create tx: %w", err)
	}
	if err := c.multiSign(tx, committeeAccountName); err != nil {
		return fmt.Errorf("can't sign a transaction: %w", err)
	}

	network := c.CommitteeAct.GetNetwork()
	for i := range c.Accounts {
		if err := c.Accounts[i].SignTx(network, tx); err != nil {
			return fmt.Errorf("can't sign a transaction: %w", err)
		}
	}

	return c.sendTx(tx, c.Command, true)
}

func (c *initializeContext) transferNEOToAlphabetContracts() error {
	neoHash := neo.Hash

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

	if err := c.sendCommitteeTx(bw.Bytes(), false); err != nil {
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
	case *rpcclient.Client:
		return ct.GetCandidateRegisterPrice()
	default:
		neoHash := neo.Hash
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
