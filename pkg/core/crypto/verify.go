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

type signedDataFunc = func() []byte

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

// VerifyTokenSignature verifies t signature.
func VerifyTokenSignature[T interface {
	Signature() (neofscrypto.Signature, bool)
	SignedData() []byte
}](t T) (*ecdsa.PublicKey, error) {
	sig, ok := t.Signature()
	if !ok {
		return nil, errors.New("no signature")
	}
	var pub *ecdsa.PublicKey
	switch scheme := sig.Scheme(); scheme {
	default:
		return nil, fmt.Errorf("unsupported scheme %v", scheme)
	case neofscrypto.ECDSA_SHA512:
		var err error
		if pub, err = verifyECDSASHA512Signature(sig.PublicKeyBytes(), sig.Value(), t.SignedData); err != nil {
			return nil, fmt.Errorf("%s scheme: %w", neofscrypto.ECDSA_SHA512, err)
		}
	case neofscrypto.ECDSA_DETERMINISTIC_SHA256:
		var err error
		if pub, err = verifyECDSARFC6979Signature(sig.PublicKeyBytes(), sig.Value(), t.SignedData); err != nil {
			return nil, fmt.Errorf("%s scheme: %w", neofscrypto.ECDSA_DETERMINISTIC_SHA256, err)
		}
	case neofscrypto.ECDSA_WALLETCONNECT:
		var err error
		if pub, err = verifyECDSAWalletConnectSignature(sig.PublicKeyBytes(), sig.Value(), t.SignedData); err != nil {
			return nil, fmt.Errorf("%s scheme: %w", neofscrypto.ECDSA_WALLETCONNECT, err)
		}
	}
	return pub, nil
}
