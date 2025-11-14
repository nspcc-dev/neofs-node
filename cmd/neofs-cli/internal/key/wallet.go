package key

import (
	"crypto/ecdsa"
	"errors"

	"github.com/nspcc-dev/neo-go/cli/flags"
	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Key-related errors.
var (
	ErrFs              = errors.New("unable to read file from given path")
	ErrInvalidKey      = errors.New("provided wallet is incorrect")
	ErrInvalidAddress  = errors.New("--address option must be specified and valid")
	ErrInvalidPassword = errors.New("invalid password for the encrypted key")
)

// FromWallet returns private key of the wallet account.
func FromWallet(cmd *cobra.Command, w *wallet.Wallet, addrStr string) (*ecdsa.PrivateKey, error) {
	var (
		addr util.Uint160
		err  error
	)

	if addrStr == "" {
		common.PrintVerbose(cmd, "Using default wallet address")
		addr = w.GetChangeAddress()
	} else {
		addr, err = flags.ParseAddress(addrStr)
		if err != nil {
			common.PrintVerbose(cmd, "Can't parse address: %s", addrStr)
			return nil, ErrInvalidAddress
		}
	}

	acc := w.GetAccount(addr)
	if acc == nil {
		common.PrintVerbose(cmd, "Can't find wallet account for %s", addrStr)
		return nil, ErrInvalidAddress
	}

	pass, err := getPassword()
	if err != nil {
		common.PrintVerbose(cmd, "Can't read password: %v", err)
		return nil, ErrInvalidPassword
	}

	if err := acc.Decrypt(pass, w.Scrypt); err != nil {
		common.PrintVerbose(cmd, "Can't decrypt account: %v", err)
		return nil, ErrInvalidPassword
	}

	return &acc.PrivateKey().PrivateKey, nil
}

func getPassword() (string, error) {
	// this check allows empty passwords
	if viper.IsSet("password") {
		return viper.GetString("password"), nil
	}

	return input.ReadPassword("Enter password > ")
}
