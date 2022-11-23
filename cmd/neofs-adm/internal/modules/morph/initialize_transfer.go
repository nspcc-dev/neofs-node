package morph

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/neo"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	scContext "github.com/nspcc-dev/neo-go/pkg/smartcontract/context"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
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

	var transfers []rpcclient.TransferTarget
	for _, acc := range c.Accounts {
		to := acc.Contract.ScriptHash()
		transfers = append(transfers,
			rpcclient.TransferTarget{
				Token:   gas.Hash,
				Address: to,
				Amount:  initialAlphabetGASAmount,
			},
		)
	}

	// It is convenient to have all funds at the committee account.
	transfers = append(transfers,
		rpcclient.TransferTarget{
			Token:   gas.Hash,
			Address: c.CommitteeAcc.Contract.ScriptHash(),
			Amount:  (gasInitialTotalSupply - initialAlphabetGASAmount*int64(len(c.Wallets))) / 2,
		},
		rpcclient.TransferTarget{
			Token:   neo.Hash,
			Address: c.CommitteeAcc.Contract.ScriptHash(),
			Amount:  native.NEOTotalSupply,
		},
	)

	tx, err := createNEP17MultiTransferTx(c.Client, c.ConsensusAcc, 0, transfers, []rpcclient.SignerAccount{{
		Signer: transaction.Signer{
			Account: c.ConsensusAcc.Contract.ScriptHash(),
			Scopes:  transaction.CalledByEntry,
		},
		Account: c.ConsensusAcc,
	}})
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

	res, err := c.Client.NEP17BalanceOf(gas.Hash, acc.Contract.ScriptHash())
	return res > initialAlphabetGASAmount/2, err
}

func (c *initializeContext) multiSignAndSend(tx *transaction.Transaction, accType string) error {
	if err := c.multiSign(tx, accType); err != nil {
		return err
	}

	return c.sendTx(tx, c.Command, false)
}

func (c *initializeContext) multiSign(tx *transaction.Transaction, accType string) error {
	network, err := c.Client.GetNetwork()
	if err != nil {
		// error appears only if client
		// has not been initialized
		panic(err)
	}

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

	bal, err := c.Client.NEP17BalanceOf(gas.Hash, proxyCs.Hash)
	if err != nil || bal > 0 {
		return err
	}

	tx, err := createNEP17MultiTransferTx(c.Client, c.CommitteeAcc, 0, []rpcclient.TransferTarget{{
		Token:   gas.Hash,
		Address: proxyCs.Hash,
		Amount:  initialProxyGASAmount,
	}}, nil)
	if err != nil {
		return err
	}

	if err := c.multiSignAndSend(tx, committeeAccountName); err != nil {
		return err
	}

	return c.awaitTx()
}

func createNEP17MultiTransferTx(c Client, acc *wallet.Account, netFee int64,
	recipients []rpcclient.TransferTarget, cosigners []rpcclient.SignerAccount) (*transaction.Transaction, error) {
	from := acc.Contract.ScriptHash()

	w := io.NewBufBinWriter()
	for i := range recipients {
		emit.AppCall(w.BinWriter, recipients[i].Token, "transfer", callflag.All,
			from, recipients[i].Address, recipients[i].Amount, recipients[i].Data)
		emit.Opcodes(w.BinWriter, opcode.ASSERT)
	}
	if w.Err != nil {
		return nil, fmt.Errorf("failed to create transfer script: %w", w.Err)
	}
	return c.CreateTxFromScript(w.Bytes(), acc, -1, netFee, append([]rpcclient.SignerAccount{{
		Signer: transaction.Signer{
			Account: from,
			Scopes:  transaction.CalledByEntry,
		},
		Account: acc,
	}}, cosigners...))
}
