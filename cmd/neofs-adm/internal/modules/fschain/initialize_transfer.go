package fschain

import (
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/neo"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep17"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	scContext "github.com/nspcc-dev/neo-go/pkg/smartcontract/context"
)

const (
	gasInitialTotalSupply = 30000000 * native.GASFactor
	// initialAlphabetGASAmount represents the amount of GAS given to each alphabet node.
	initialAlphabetGASAmount = 10_000 * native.GASFactor
	// initialProxyGASAmount represents the amount of GAS given to a proxy contract.
	initialProxyGASAmount = 50_000 * native.GASFactor
)

func (c *initializeContext) transferFunds() error {
	ok, err := c.transferFundsFinished()
	if ok || err != nil {
		if err == nil {
			c.Command.Println("Stage 1: already performed.")
		}
		return err
	}

	from := c.ConsensusAcc.Contract.ScriptHash()
	b := smartcontract.NewBuilder()

	for _, acc := range c.Accounts {
		b.InvokeWithAssert(gas.Hash, "transfer", from, acc.Contract.ScriptHash(), initialAlphabetGASAmount, nil)
	}

	// It is convenient to have all funds at the committee account.
	b.InvokeWithAssert(gas.Hash, "transfer", from, c.CommitteeAcc.Contract.ScriptHash(),
		(gasInitialTotalSupply-initialAlphabetGASAmount*int64(len(c.Wallets)))/2, nil)

	b.InvokeWithAssert(neo.Hash, "transfer", from,
		c.CommitteeAcc.Contract.ScriptHash(), native.NEOTotalSupply, nil)

	act, err := actor.NewSimple(c.Client, c.ConsensusAcc)
	if err != nil {
		return fmt.Errorf("creating actor: %w", err)
	}

	s, err := b.Script()
	if err != nil {
		return fmt.Errorf("multitransfer script creation: %w", err)
	}

	tx, err := act.MakeUnsignedRun(s, nil)
	if err != nil {
		return fmt.Errorf("can't create transfer transaction: %w", err)
	}

	if err := c.multiSignAndSend(tx, consensusAccountName); err != nil {
		return fmt.Errorf("can't send transfer transaction: %w", err)
	}

	return c.awaitTx()
}

func (c *initializeContext) transferFundsFinished() (bool, error) {
	acc := c.Accounts[0]

	gasR := gas.NewReader(invoker.New(c.Client, nil))
	res, err := gasR.BalanceOf(acc.Contract.ScriptHash())
	if err != nil {
		return false, err
	}
	return res.Int64() > initialAlphabetGASAmount/2, nil
}

func (c *initializeContext) multiSignAndSend(tx *transaction.Transaction, accType string) error {
	if err := c.multiSign(tx, accType); err != nil {
		return err
	}

	return c.sendTx(tx, c.Command, false)
}

func (c *initializeContext) multiSign(tx *transaction.Transaction, accType string) error {
	network := c.CommitteeAct.GetNetwork()

	// Use parameter context to avoid dealing with signature order.
	pc := scContext.NewParameterContext("", network, tx)
	h := c.CommitteeAcc.Contract.ScriptHash()
	if accType == consensusAccountName {
		h = c.ConsensusAcc.Contract.ScriptHash()
	}
	for _, w := range c.Wallets {
		acc, err := getWalletAccount(w, accType)
		if err != nil {
			return fmt.Errorf("can't find %s wallet account: %w", accType, err)
		}

		priv := acc.PrivateKey()
		sign := priv.SignHashable(uint32(network), tx)
		if err := pc.AddSignature(h, acc.Contract, priv.PublicKey(), sign); err != nil {
			return fmt.Errorf("can't add signature: %w", err)
		}
		if len(pc.Items[h].Signatures) == len(acc.Contract.Parameters) {
			break
		}
	}

	w, err := pc.GetWitness(h)
	if err != nil {
		return fmt.Errorf("incomplete signature: %w", err)
	}

	for i := range tx.Signers {
		if tx.Signers[i].Account == h {
			if i < len(tx.Scripts) {
				tx.Scripts[i] = *w
			} else if i == len(tx.Scripts) {
				tx.Scripts = append(tx.Scripts, *w)
			} else {
				panic("BUG: invalid signing order")
			}
			return nil
		}
	}

	return fmt.Errorf("%s account was not found among transaction signers", accType)
}

func (c *initializeContext) transferGASToProxy() error {
	proxyCs := c.getContract(proxyContract)

	gasR := gas.NewReader(invoker.New(c.Client, nil))
	bal, err := gasR.BalanceOf(proxyCs.Hash)
	if err != nil || bal.Sign() > 0 {
		return err
	}

	from := c.CommitteeAcc.Contract.ScriptHash()

	gToken := nep17.New(c.CommitteeAct, gas.Hash)
	tx, err := gToken.MultiTransferUnsigned([]nep17.TransferParameters{{
		From:   from,
		To:     proxyCs.Hash,
		Amount: big.NewInt(initialProxyGASAmount),
	}})
	if err != nil {
		return err
	}

	if err := c.multiSignAndSend(tx, committeeAccountName); err != nil {
		return err
	}

	return c.awaitTx()
}
