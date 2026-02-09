package n3util

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/config"
	"github.com/spf13/viper"
)

// GetWalletAccount returns account with the given label from the wallet.
func GetWalletAccount(w *wallet.Wallet, typ string) (*wallet.Account, error) {
	for i := range w.Accounts {
		if w.Accounts[i].Label == typ {
			return w.Accounts[i], nil
		}
	}
	return nil, fmt.Errorf("account for '%s' not found", typ)
}

// OpenAlphabetWallets opens all wallets from the given directory and returns them as a slice.
func OpenAlphabetWallets(v *viper.Viper, walletDir string) ([]*wallet.Wallet, error) {
	walletFiles, err := os.ReadDir(walletDir)
	if err != nil {
		return nil, fmt.Errorf("can't read alphabet wallets dir: %w", err)
	}

	var wallets []*wallet.Wallet

	for _, walletFile := range walletFiles {
		isJson := strings.HasSuffix(walletFile.Name(), ".json")
		if walletFile.IsDir() || !isJson {
			continue // Ignore garbage.
		}

		w, err := OpenWallet(filepath.Join(walletDir, walletFile.Name()), v)
		if err != nil {
			return nil, fmt.Errorf("can't open wallet %s: %w", walletFile.Name(), err)
		}

		wallets = append(wallets, w)
	}
	if len(wallets) == 0 {
		return nil, errors.New("alphabet wallets dir is empty (run `generate-alphabet` command first)")
	}

	return wallets, nil
}

// OpenWallet opens a wallet from the given path and decrypts it with the password from the config.
func OpenWallet(path string, v *viper.Viper) (*wallet.Wallet, error) {
	path = config.ResolveHomePath(path)
	w, err := wallet.NewWalletFromFile(path)
	if err != nil {
		return nil, fmt.Errorf("can't open gas wallet: %w", err)
	}
	walletName := filepath.Base(path)
	walletName = strings.TrimSuffix(walletName, ".json")
	password, err := config.GetPassword(v, walletName)
	if err != nil {
		return nil, fmt.Errorf("can't fetch password: %w", err)
	}
	for i := range w.Accounts {
		if err := w.Accounts[i].Decrypt(password, w.Scrypt); err != nil {
			return nil, fmt.Errorf("can't unlock wallet: %w", err)
		}
	}
	return w, nil
}
