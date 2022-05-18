package key

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"os"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/viper"
)

var errCantGenerateKey = errors.New("can't generate new private key")

// Get returns private key from the following sources:
// 1. WIF
// 2. Raw binary key
// 3. Wallet file
// 4. NEP-2 encrypted WIF.
// Ideally we want to touch file-system on the last step.
// However, asking for NEP-2 password seems to be confusing if we provide a wallet.
// This function assumes that all flags were bind to viper in a `PersistentPreRun`.
func Get() (*ecdsa.PrivateKey, error) {
	keyDesc := viper.GetString(commonflags.WalletPath)
	priv, err := keys.NewPrivateKeyFromWIF(keyDesc)
	if err == nil {
		return &priv.PrivateKey, nil
	}

	p, err := getKeyFromFile(keyDesc)
	if err == nil {
		return p, nil
	}

	w, err := wallet.NewWalletFromFile(keyDesc)
	if err == nil {
		return FromWallet(w, viper.GetString(commonflags.Account))
	}

	if len(keyDesc) == nep2Base58Length {
		return FromNEP2(keyDesc)
	}

	return nil, ErrInvalidKey
}

// GetOrGenerate is similar to get but generates a new key if commonflags.GenerateKey is set.
func GetOrGenerate() (*ecdsa.PrivateKey, error) {
	if viper.GetBool(commonflags.GenerateKey) {
		priv, err := keys.NewPrivateKey()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errCantGenerateKey, err)
		}
		return &priv.PrivateKey, nil
	}
	return Get()
}

func getKeyFromFile(keyPath string) (*ecdsa.PrivateKey, error) {
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidKey, err)
	}

	priv, err := keys.NewPrivateKeyFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidKey, err)
	}

	return &priv.PrivateKey, nil
}
