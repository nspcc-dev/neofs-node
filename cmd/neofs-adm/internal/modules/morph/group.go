package morph

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	contractWalletFilename    = "contract.json"
	contractWalletPasswordKey = "credentials.contract"
)

func initializeContractWallet(walletDir string) (*wallet.Wallet, error) {
	password, err := getPassword(contractWalletPasswordKey, "Password for contract wallet > ")
	if err != nil {
		return nil, err
	}

	w, err := wallet.NewWallet(path.Join(walletDir, contractWalletFilename))
	if err != nil {
		return nil, err
	}

	acc, err := wallet.NewAccount()
	if err != nil {
		return nil, err
	}

	err = acc.Encrypt(password, keys.NEP2ScryptParams())
	if err != nil {
		return nil, err
	}

	w.AddAccount(acc)
	if err := w.Save(); err != nil {
		return nil, err
	}

	return w, nil
}

func openContractWallet(cmd *cobra.Command, walletDir string) (*wallet.Wallet, error) {
	p := path.Join(walletDir, contractWalletFilename)
	w, err := wallet.NewWalletFromFile(p)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("can't open wallet: %w", err)
		}

		cmd.Printf("Contract group wallet is missing, initialize at %s\n",
			filepath.Join(walletDir, contractWalletFilename))
		w, err = initializeContractWallet(walletDir)
		if err != nil {
			return nil, err
		}
	}

	password, err := getPassword(contractWalletPasswordKey, "Password for contract wallet > ")
	if err != nil {
		return nil, err
	}

	for i := range w.Accounts {
		if err := w.Accounts[i].Decrypt(password, keys.NEP2ScryptParams()); err != nil {
			return nil, fmt.Errorf("can't unlock wallet: %w", err)
		}
	}

	return w, nil
}

func getPassword(key string, promps string) (string, error) {
	if viper.IsSet(key) {
		return viper.GetString(key), nil
	}

	prompt := "Password for contract wallet > "
	return input.ReadPassword(prompt)
}

func (c *initializeContext) addManifestGroup(h util.Uint160, cs *contractState) error {
	priv := c.ContractWallet.Accounts[0].PrivateKey()
	pub := priv.PublicKey()

	sig := priv.Sign(h.BytesBE())
	found := false

	for i := range cs.Manifest.Groups {
		if cs.Manifest.Groups[i].PublicKey.Equal(pub) {
			cs.Manifest.Groups[i].Signature = sig
			found = true
			break
		}
	}
	if !found {
		cs.Manifest.Groups = append(cs.Manifest.Groups, manifest.Group{
			PublicKey: pub,
			Signature: sig,
		})
	}

	data, err := json.Marshal(cs.Manifest)
	if err != nil {
		return err
	}

	cs.RawManifest = data
	return nil
}
