package key

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
)

const nep2Base58Length = 58

// FromNEP2 extracts private key from NEP2-encrypted string.
func FromNEP2(encryptedWif string) (*ecdsa.PrivateKey, error) {
	pass, err := getPassword()
	if err != nil {
		printVerbose("Can't read password: %v", err)
		return nil, ErrInvalidPassword
	}

	k, err := keys.NEP2Decrypt(encryptedWif, pass, keys.NEP2ScryptParams())
	if err != nil {
		printVerbose("Invalid key or password: %v", err)
		return nil, ErrInvalidPassword
	}

	return &k.PrivateKey, nil
}
