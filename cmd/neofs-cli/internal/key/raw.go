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
func Get(cmd *cobra.Command) (*ecdsa.PrivateKey, error) {
	pk, err := get(cmd)
	if err != nil {
		return nil, fmt.Errorf("can't fetch private key: %w", err)
	}
	return pk, nil
}

var ErrMissingFlag = errors.New("missing key flag")

// get decodes ecdsa.PrivateKey from the wallet file located in the path
// stored by commonflags.WalletPath viper key. Returns errMissingFlag
// if the viper flag is not set.
func get(cmd *cobra.Command) (*ecdsa.PrivateKey, error) {
	keyDesc := viper.GetString(commonflags.WalletPath)
	if keyDesc == "" {
		return nil, ErrMissingFlag
	}
	w, err := wallet.NewWalletFromFile(keyDesc)
	if err != nil {
		var perr = new(*os.PathError)
		if errors.As(err, perr) {
			return nil, fmt.Errorf("%w: %w", ErrFs, err)
		}
		return nil, fmt.Errorf("%w: %w", ErrInvalidKey, err)
	}
	return FromWallet(cmd, w, viper.GetString(commonflags.Account))
}

// GetOrGenerate is similar to get but generates a new key if commonflags.GenerateKey is set.
func GetOrGenerate(cmd *cobra.Command) (*ecdsa.PrivateKey, error) {
	pk, err := getOrGenerate(cmd)
	if err != nil {
		return nil, fmt.Errorf("can't fetch private key: %w", err)
	}
	return pk, nil
}

func getOrGenerate(cmd *cobra.Command) (*ecdsa.PrivateKey, error) {
	if !viper.GetBool(commonflags.GenerateKey) {
		key, err := get(cmd)
		if !errors.Is(err, ErrMissingFlag) {
			if err == nil {
				common.PrintVerbose(cmd, "Configured wallet will be used for the command.")
			}

			return key, err
		}

		common.PrintVerbose(cmd, "Missing wallet in the configuration.")
	}

	common.PrintVerbose(cmd, "Generating random private key for command processing...")

	priv, err := keys.NewPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errCantGenerateKey, err)
	}

	common.PrintVerbose(cmd, "Private key generated successfully. Public key: %s", priv.PublicKey())

	return &priv.PrivateKey, nil
}
