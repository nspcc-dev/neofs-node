package morph

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	scContext "github.com/nspcc-dev/neo-go/pkg/smartcontract/context"
)

const (
	gasInitialTotalSupply = 30000000 * native.GASFactor
	// initialAlphabetGASAmount represents amount of GAS given to each alphabet node.
	initialAlphabetGASAmount = 10_000 * native.GASFactor
	// initialProxyGASAmount represents amount of GAS given to proxy contract.
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

	gasHash := c.nativeHash(nativenames.Gas)
	neoHash := c.nativeHash(nativenames.Neo)

	var transfers []client.TransferTarget
	for _, acc := range c.Accounts {
		to := acc.Contract.ScriptHash()
		transfers = append(transfers,
			client.TransferTarget{
				Token:   gasHash,
				Address: to,
				Amount:  initialAlphabetGASAmount,
			},
		)
	}

	// It is convenient to have all funds at the committee account.
	transfers = append(transfers,
		client.TransferTarget{
			Token:   gasHash,
			Address: c.CommitteeAcc.Contract.ScriptHash(),
			Amount:  (gasInitialTotalSupply - initialAlphabetGASAmount*int64(len(c.Wallets))) / 2,
		},
		client.TransferTarget{
			Token:   neoHash,
			Address: c.CommitteeAcc.Contract.ScriptHash(),
			Amount:  native.NEOTotalSupply,
		},
	)

	tx, err := c.Client.CreateNEP17MultiTransferTx(c.ConsensusAcc, 0, transfers, []client.SignerAccount{{
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
	gasHash := c.nativeHash(nativenames.Gas)
	acc := c.Accounts[0]

	res, err := c.Client.NEP17BalanceOf(gasHash, acc.Contract.ScriptHash())
	return res > initialAlphabetGASAmount/2, err
}

func (c *initializeContext) multiSignAndSend(tx *transaction.Transaction, accType string) error {
	if err := c.multiSign(tx, accType); err != nil {
		return err
	}

	return c.sendTx(tx, c.Command, false)
}

func (c *initializeContext) multiSign(tx *transaction.Transaction, accType string) error {
	network := c.Client.GetNetwork()

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

	w, err := pc.GetWitness(tx.Signers[0].Account)
	if err != nil {
		return fmt.Errorf("incomplete signature: %w", err)
	}
	tx.Scripts = append(tx.Scripts, *w)

	return nil
}

func (c *initializeContext) transferGASToProxy() error {
	gasHash := c.nativeHash(nativenames.Gas)
	proxyCs := c.getContract(proxyContract)

	bal, err := c.Client.NEP17BalanceOf(gasHash, proxyCs.Hash)
	if err != nil || bal > 0 {
		return err
	}

	tx, err := c.Client.CreateNEP17TransferTx(c.CommitteeAcc, proxyCs.Hash, gasHash, initialProxyGASAmount, 0, nil, nil)
	if err != nil {
		return err
	}

	if err := c.multiSignAndSend(tx, committeeAccountName); err != nil {
		return err
	}

	return c.awaitTx()
}
