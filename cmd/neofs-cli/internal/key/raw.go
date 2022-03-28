package key

import (
	"crypto/ecdsa"
	"fmt"
	"os"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
)

// Get returns private key from the followind sources:
// 1. WIF
// 2. Raw binary key
// 3. Wallet file
// 4. NEP-2 encrypted WIF.
// Ideally we want to touch file-system on the last step.
// However, asking for NEP-2 password seems to be confusing if we provide a wallet.
func Get(keyDesc string, address string) (*ecdsa.PrivateKey, error) {
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
		return FromWallet(w, address)
	}

	if len(keyDesc) == nep2Base58Length {
		return FromNEP2(keyDesc)
	}

	return nil, ErrInvalidKey
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
