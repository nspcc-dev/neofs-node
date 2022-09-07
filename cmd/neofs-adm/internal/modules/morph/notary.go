package morph

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep17"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// defaultNotaryDepositLifetime is an amount of blocks notary deposit stays valid.
// https://github.com/nspcc-dev/neo-go/blob/master/pkg/core/native/notary.go#L48
const defaultNotaryDepositLifetime = 5760

func depositNotary(cmd *cobra.Command, _ []string) error {
	p, err := cmd.Flags().GetString(storageWalletFlag)
	if err != nil {
		return err
	} else if p == "" {
		return fmt.Errorf("missing wallet path (use '--%s <out.json>')", storageWalletFlag)
	}

	w, err := wallet.NewWalletFromFile(p)
	if err != nil {
		return fmt.Errorf("can't open wallet: %v", err)
	}

	accHash := w.GetChangeAddress()
	if addr, err := cmd.Flags().GetString(walletAccountFlag); err == nil {
		accHash, err = address.StringToUint160(addr)
		if err != nil {
			return fmt.Errorf("invalid address: %s", addr)
		}
	}

	acc := w.GetAccount(accHash)
	if acc == nil {
		return fmt.Errorf("can't find account for %s", accHash)
	}

	prompt := fmt.Sprintf("Enter password for %s >", address.Uint160ToString(accHash))
	pass, err := input.ReadPassword(prompt)
	if err != nil {
		return fmt.Errorf("can't get password: %v", err)
	}

	err = acc.Decrypt(pass, keys.NEP2ScryptParams())
	if err != nil {
		return fmt.Errorf("can't unlock account: %v", err)
	}

	gasStr, err := cmd.Flags().GetString(refillGasAmountFlag)
	if err != nil {
		return err
	}
	gasAmount, err := parseGASAmount(gasStr)
	if err != nil {
		return err
	}

	till := int64(defaultNotaryDepositLifetime)
	tillStr, err := cmd.Flags().GetString(notaryDepositTillFlag)
	if err != nil {
		return err
	}
	if tillStr != "" {
		till, err = strconv.ParseInt(tillStr, 10, 64)
		if err != nil || till <= 0 {
			return fmt.Errorf("notary deposit lifetime must be a positive integer")
		}
	}

	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return err
	}

	nhs, err := getNativeHashes(c)
	if err != nil {
		return fmt.Errorf("can't get native contract hashes: %w", err)
	}

	gasHash, ok := nhs[nativenames.Gas]
	if !ok {
		return fmt.Errorf("can't retrieve %s contract hash", nativenames.Gas)
	}
	notaryHash, ok := nhs[nativenames.Notary]
	if !ok {
		return fmt.Errorf("can't retrieve %s contract hash", nativenames.Notary)
	}

	height, err := c.GetBlockCount()
	if err != nil {
		return fmt.Errorf("can't get current height: %v", err)
	}

	act, err := actor.New(c, []actor.SignerAccount{{
		Signer: transaction.Signer{
			Account: acc.Contract.ScriptHash(),
			Scopes:  transaction.Global,
		},
		Account: acc,
	}})
	if err != nil {
		return fmt.Errorf("could not create actor: %w", err)
	}

	gas := nep17.New(act, gasHash)

	txHash, _, err := gas.Transfer(
		accHash,
		notaryHash,
		big.NewInt(int64(gasAmount)),
		[]interface{}{nil, int64(height) + till},
	)
	if err != nil {
		return fmt.Errorf("could not send tx: %w", err)
	}

	return awaitTx(cmd, c, []util.Uint256{txHash})
}
