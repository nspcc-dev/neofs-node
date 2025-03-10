package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
)

func verifyECDSASignature(pubBin, sig []byte, signedData signedDataFunc, castPub func(*ecdsa.PublicKey) neofscrypto.PublicKey) (*ecdsa.PublicKey, error) {
	pub, err := keys.NewPublicKeyFromBytes(pubBin, elliptic.P256())
	if err != nil {
		return nil, fmt.Errorf("decode public key: %w", err)
	}
	if !castPub((*ecdsa.PublicKey)(pub)).Verify(signedData(), sig) {
		return nil, errors.New("signature mismatch")
	}
	return (*ecdsa.PublicKey)(pub), nil
}

func verifyECDSASHA512Signature(pubBin, sig []byte, signedDataFn signedDataFunc) (*ecdsa.PublicKey, error) {
	return verifyECDSASignature(pubBin, sig, signedDataFn, func(pub *ecdsa.PublicKey) neofscrypto.PublicKey {
		return (*neofsecdsa.PublicKey)(pub)
	})
}

func verifyECDSARFC6979Signature(pubBin, sig []byte, signedDataFn signedDataFunc) (*ecdsa.PublicKey, error) {
	return verifyECDSASignature(pubBin, sig, signedDataFn, func(pub *ecdsa.PublicKey) neofscrypto.PublicKey {
		return (*neofsecdsa.PublicKeyRFC6979)(pub)
	})
}

func verifyECDSAWalletConnectSignature(pubBin, sig []byte, signedDataFn signedDataFunc) (*ecdsa.PublicKey, error) {
	return verifyECDSASignature(pubBin, sig, signedDataFn, func(pub *ecdsa.PublicKey) neofscrypto.PublicKey {
		return (*neofsecdsa.PublicKeyWalletConnect)(pub)
	})
}
