package fschain

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	scContext "github.com/nspcc-dev/neo-go/pkg/smartcontract/context"
)

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
