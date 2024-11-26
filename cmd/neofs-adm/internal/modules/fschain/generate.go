package fschain

import (
	"errors"
	"fmt"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep17"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/config"
	"github.com/nspcc-dev/neofs-node/pkg/util/glagolitsa"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	singleAccountName    = "single"
	committeeAccountName = "committee"
	consensusAccountName = "consensus"
)

func generateAlphabetCreds(cmd *cobra.Command, _ []string) error {
	// alphabet size is not part of the config
	size, err := cmd.Flags().GetUint(alphabetSizeFlag)
	if err != nil {
		return err
	}
	if size == 0 {
		return errors.New("size must be > 0")
	}

	v := viper.GetViper()
	walletDir := config.ResolveHomePath(viper.GetString(alphabetWalletsFlag))
	pwds, err := initializeWallets(v, walletDir, int(size))
	if err != nil {
		return err
	}

	cmd.Println("size:", size)
	cmd.Println("alphabet-wallets:", walletDir)
	for i := range pwds {
		cmd.Printf("wallet[%d]: %s\n", i, pwds[i])
	}

	return nil
}

func initializeWallets(v *viper.Viper, walletDir string, size int) ([]string, error) {
	wallets := make([]*wallet.Wallet, size)
	pubs := make(keys.PublicKeys, size)
	passwords := make([]string, size)

	for i := range wallets {
		letter := glagolitsa.LetterByIndex(i)
		password, err := config.GetPassword(v, letter)
		if err != nil {
			return nil, fmt.Errorf("can't fetch password: %w", err)
		}

		p := filepath.Join(walletDir, letter+".json")
		f, err := os.OpenFile(p, os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("can't create wallet file: %w", err)
		}
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("can't close wallet file: %w", err)
		}
		w, err := wallet.NewWallet(p)
		if err != nil {
			return nil, fmt.Errorf("can't create wallet: %w", err)
		}
		if err := w.CreateAccount(singleAccountName, password); err != nil {
			return nil, fmt.Errorf("can't create account: %w", err)
		}

		passwords[i] = password
		wallets[i] = w
		pubs[i] = w.Accounts[0].PublicKey()
	}

	// Create committee account with N/2+1 multi-signature.
	majCount := smartcontract.GetMajorityHonestNodeCount(size)
	for i, w := range wallets {
		if err := addMultisigAccount(w, majCount, committeeAccountName, passwords[i], pubs); err != nil {
			return nil, fmt.Errorf("can't create committee account: %w", err)
		}
	}

	// Create consensus account with 2*N/3+1 multi-signature.
	bftCount := smartcontract.GetDefaultHonestNodeCount(size)
	for i, w := range wallets {
		if err := addMultisigAccount(w, bftCount, consensusAccountName, passwords[i], pubs); err != nil {
			return nil, fmt.Errorf("can't create consensus account: %w", err)
		}
	}

	for _, w := range wallets {
		if err := w.SavePretty(); err != nil {
			return nil, fmt.Errorf("can't save wallet: %w", err)
		}
	}

	return passwords, nil
}

func addMultisigAccount(w *wallet.Wallet, m int, name, password string, pubs keys.PublicKeys) error {
	acc := wallet.NewAccountFromPrivateKey(w.Accounts[0].PrivateKey())
	acc.Label = name

	if err := acc.ConvertMultisig(m, pubs); err != nil {
		return err
	}
	if err := acc.Encrypt(password, keys.NEP2ScryptParams()); err != nil {
		return err
	}
	w.AddAccount(acc)
	return nil
}

func generateStorageCreds(cmd *cobra.Command, _ []string) error {
	// storage wallet path is not part of the config
	storageWalletPath, _ := cmd.Flags().GetString(storageWalletFlag)
	if storageWalletPath == "" {
		return fmt.Errorf("missing wallet path (use '--%s <out.json>')", storageWalletFlag)
	}

	walletsNumber, err := cmd.Flags().GetUint32(storageWalletsNumber)
	if err != nil {
		return err
	}
	if walletsNumber == 0 {
		walletsNumber = 1
	}

	label, _ := cmd.Flags().GetString(storageWalletLabelFlag)
	password, err := config.GetStoragePassword(viper.GetViper(), label)
	if err != nil {
		return fmt.Errorf("can't fetch password: %w", err)
	}

	if label == "" {
		label = singleAccountName
	}

	hashes, err := createWallets(storageWalletPath, label, password, walletsNumber)
	if err != nil {
		return err
	}

	gasAmount, err := parseGASAmount(viper.GetString(storageGasConfigFlag))
	if err != nil {
		return err
	}

	return refillGas(cmd, int64(gasAmount), hashes)
}

func refillGas(cmd *cobra.Command, gasAmount int64, receivers []util.Uint160) (err error) {
	wCtx, err := newInitializeContext(cmd, viper.GetViper())
	if err != nil {
		return err
	}

	committeeScriptHash := wCtx.CommitteeAcc.Contract.ScriptHash()

	var pp []nep17.TransferParameters
	for _, receiver := range receivers {
		pp = append(pp, nep17.TransferParameters{
			From:   committeeScriptHash,
			To:     receiver,
			Amount: big.NewInt(gasAmount),
		})
	}

	gToken := nep17.New(wCtx.CommitteeAct, gas.Hash)
	tx, err := gToken.MultiTransferUnsigned(pp)
	if err != nil {
		return err
	}

	if err := wCtx.multiSignAndSend(tx, committeeAccountName); err != nil {
		return err
	}

	return wCtx.awaitTx()
}

func parseGASAmount(s string) (fixedn.Fixed8, error) {
	gasAmount, err := fixedn.Fixed8FromString(s)
	if err != nil {
		return 0, fmt.Errorf("invalid GAS amount %s: %w", s, err)
	}
	if gasAmount <= 0 {
		return 0, fmt.Errorf("GAS amount must be positive (got %d)", gasAmount)
	}
	return gasAmount, nil
}

func createWallets(fileNameTemplate, label, password string, number uint32) ([]util.Uint160, error) {
	var res []util.Uint160
	ext := path.Ext(fileNameTemplate)
	base := strings.TrimSuffix(fileNameTemplate, ext)
	walletNumberFormat := fmt.Sprintf("%%0%dd", digitsNum(number))

	for i := range int(number) {
		filename := fileNameTemplate
		if number != 1 {
			filename = base + "_" + fmt.Sprintf(walletNumberFormat, i) + ext
		}

		w, err := wallet.NewWallet(filename)
		if err != nil {
			return nil, fmt.Errorf("wallet creation: %w", err)
		}

		err = w.CreateAccount(label, password)
		if err != nil {
			return nil, fmt.Errorf("account creation: %w", err)
		}

		res = append(res, w.Accounts[0].Contract.ScriptHash())
	}

	return res, nil
}

func digitsNum(val uint32) int {
	var res int
	for val != 0 {
		val /= 10
		res++
	}

	return res
}
