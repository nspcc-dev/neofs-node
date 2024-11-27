package fschain

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/neo"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep17"
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
		emit.AppCall(w.BinWriter, neoHash, "registerCandidate", callflag.States, acc.PublicKey().Bytes())
		emit.Opcodes(w.BinWriter, opcode.ASSERT)
	}
	emit.AppCall(w.BinWriter, neoHash, "setRegisterPrice", callflag.States, regPrice)
	if w.Err != nil {
		panic(fmt.Sprintf("BUG: %v", w.Err))
	}

	signers := []actor.SignerAccount{{
		Signer: transaction.Signer{
			Account: c.CommitteeAcc.Contract.ScriptHash(),
			Scopes:  transaction.CalledByEntry,
		},
		Account: c.CommitteeAcc,
	}}
	for i := range c.Accounts {
		signers = append(signers, actor.SignerAccount{
			Signer: transaction.Signer{
				Account:          c.Accounts[i].Contract.ScriptHash(),
				Scopes:           transaction.CustomContracts,
				AllowedContracts: []util.Uint160{neoHash},
			},
			Account: c.Accounts[i],
		})
	}
	act, err := actor.New(c.Client, signers)
	if err != nil {
		return fmt.Errorf("creating actor: %w", err)
	}

	tx, err := act.MakeUnsignedRun(w.Bytes(), nil)
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
	ok, err := c.transferNEOFinished()
	if ok || err != nil {
		return err
	}

	cs := c.getContract(alphabetContract)
	amount := initialAlphabetNEOAmount / len(c.Wallets)

	tNeo := nep17.New(c.CommitteeAct, neo.Hash)
	pp := make([]nep17.TransferParameters, 0, len(c.Accounts))

	for _, acc := range c.Accounts {
		h := state.CreateContractHash(acc.Contract.ScriptHash(), cs.NEF.Checksum, cs.Manifest.Name)

		pp = append(pp, nep17.TransferParameters{
			From:   c.CommitteeAcc.Contract.ScriptHash(),
			To:     h,
			Amount: big.NewInt(int64(amount)),
		})
	}

	tx, err := tNeo.MultiTransferUnsigned(pp)
	if err != nil {
		return fmt.Errorf("multi-transfer script: %w", err)
	}

	if err := c.multiSignAndSend(tx, committeeAccountName); err != nil {
		return err
	}

	return c.awaitTx()
}

func (c *initializeContext) transferNEOFinished() (bool, error) {
	neoR := neo.NewReader(invoker.New(c.Client, nil))
	bal, err := neoR.BalanceOf(c.CommitteeAcc.Contract.ScriptHash())
	if err != nil {
		return false, err
	}
	return bal.Int64() < native.NEOTotalSupply, nil
}

var errGetPriceInvalid = errors.New("`getRegisterPrice`: invalid response")

func (c *initializeContext) getCandidateRegisterPrice() (int64, error) {
	switch c.Client.(type) {
	case *rpcclient.Client:
		reader := neo.NewReader(c.ReadOnlyInvoker)
		return reader.GetRegisterPrice()
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
