package key

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"os"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var errCantGenerateKey = errors.New("can't generate new private key")

// Get returns private key from wallet or binary file.
// Ideally we want to touch file-system on the last step.
// This function assumes that all flags were bind to viper in a `PersistentPreRun`.
func Get(cmd *cobra.Command) *ecdsa.PrivateKey {
	pk, err := get()
	common.ExitOnErr(cmd, "can't fetch private key: %w", err)
	return pk
}

func get() (*ecdsa.PrivateKey, error) {
	keyDesc := viper.GetString(commonflags.WalletPath)
	data, err := os.ReadFile(keyDesc)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFs, err)
	}

	priv, err := keys.NewPrivateKeyFromBytes(data)
	if err != nil {
		w, err := wallet.NewWalletFromFile(keyDesc)
		if err == nil {
			return FromWallet(w, viper.GetString(commonflags.Account))
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidKey, err)
	}
	return &priv.PrivateKey, nil
}

// GetOrGenerate is similar to get but generates a new key if commonflags.GenerateKey is set.
func GetOrGenerate(cmd *cobra.Command) *ecdsa.PrivateKey {
	pk, err := getOrGenerate()
	common.ExitOnErr(cmd, "can't fetch private key: %w", err)
	return pk
}

func getOrGenerate() (*ecdsa.PrivateKey, error) {
	if viper.GetBool(commonflags.GenerateKey) {
		priv, err := keys.NewPrivateKey()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errCantGenerateKey, err)
		}
		return &priv.PrivateKey, nil
	}
	return get()
}
