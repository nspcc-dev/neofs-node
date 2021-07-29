package morph

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
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

	gasHash, err := c.Client.GetNativeContractHash(nativenames.Gas)
	if err != nil {
		return fmt.Errorf("can't fetch %s hash: %w", nativenames.Gas, err)
	}
	neoHash, err := c.Client.GetNativeContractHash(nativenames.Neo)
	if err != nil {
		return fmt.Errorf("can't fetch %s hash: %w", nativenames.Neo, err)
	}

	var transfers []client.TransferTarget
	for _, w := range c.Wallets {
		acc, err := getWalletAccount(w, singleAccountName)
		if err != nil {
			return err
		}

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
	gasHash, err := c.Client.GetNativeContractHash(nativenames.Gas)
	if err != nil {
		return false, err
	}

	acc, err := getWalletAccount(c.Wallets[0], singleAccountName)
	if err != nil {
		return false, err
	}

	res, err := c.Client.NEP17BalanceOf(gasHash, acc.Contract.ScriptHash())
	return res > initialAlphabetGASAmount/2, err
}

func (c *initializeContext) multiSignAndSend(tx *transaction.Transaction, accType string) error {
	if err := c.multiSign(tx, accType); err != nil {
		return err
	}

	h, err := c.Client.SendRawTransaction(tx)
	if err != nil {
		return err
	}

	c.Hashes = append(c.Hashes, h)
	return nil
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
	gasHash, err := c.Client.GetNativeContractHash(nativenames.Gas)
	if err != nil {
		return fmt.Errorf("can't fetch %s hash: %w", nativenames.Gas, err)
	}

	cs, err := c.readContract(proxyContract)
	if err != nil {
		return err
	}

	h := state.CreateContractHash(c.CommitteeAcc.Contract.ScriptHash(), cs.NEF.Checksum, cs.Manifest.Name)
	bal, err := c.Client.NEP17BalanceOf(gasHash, h)
	if err != nil || bal > 0 {
		return err
	}

	tx, err := c.Client.CreateNEP17TransferTx(c.CommitteeAcc, h, gasHash, initialProxyGASAmount, 0, nil, nil)
	if err != nil {
		return err
	}

	if err := c.multiSignAndSend(tx, committeeAccountName); err != nil {
		return err
	}

	return c.awaitTx()
}
