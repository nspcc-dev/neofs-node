package morph

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	contractWalletFilename    = "contract.json"
	contractWalletPasswordKey = "contract"
)

func initializeContractWallet(v *viper.Viper, walletDir string) (*wallet.Wallet, error) {
	password, err := config.GetPassword(v, contractWalletPasswordKey)
	if err != nil {
		return nil, err
	}

	w, err := wallet.NewWallet(filepath.Join(walletDir, contractWalletFilename))
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
	if err := w.SavePretty(); err != nil {
		return nil, err
	}

	return w, nil
}

func openContractWallet(v *viper.Viper, cmd *cobra.Command, walletDir string) (*wallet.Wallet, error) {
	p := filepath.Join(walletDir, contractWalletFilename)
	w, err := wallet.NewWalletFromFile(p)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("can't open wallet: %w", err)
		}

		cmd.Printf("Contract group wallet is missing, initialize at %s\n", p)
		return initializeContractWallet(v, walletDir)
	}

	password, err := config.GetPassword(v, contractWalletPasswordKey)
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
