package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
)

type signedDataFunc = func() []byte

func verifyECDSASignature(pubBin, sig []byte, signedData signedDataFunc, castPub func(*ecdsa.PublicKey) neofscrypto.PublicKey) (*ecdsa.PublicKey, bool) {
	pub, err := keys.NewPublicKeyFromBytes(pubBin, elliptic.P256())
	if err != nil {
		return nil, false
	}
	if !castPub((*ecdsa.PublicKey)(pub)).Verify(signedData(), sig) {
		return nil, false
	}
	return (*ecdsa.PublicKey)(pub), true
}

func verifyECDSASHA512Signature(pubBin, sig []byte, signedDataFn signedDataFunc) (*ecdsa.PublicKey, bool) {
	return verifyECDSASignature(pubBin, sig, signedDataFn, func(pub *ecdsa.PublicKey) neofscrypto.PublicKey {
		return (*neofsecdsa.PublicKey)(pub)
	})
}

func verifyECDSARFC6979Signature(pubBin, sig []byte, signedDataFn signedDataFunc) (*ecdsa.PublicKey, bool) {
	return verifyECDSASignature(pubBin, sig, signedDataFn, func(pub *ecdsa.PublicKey) neofscrypto.PublicKey {
		return (*neofsecdsa.PublicKeyRFC6979)(pub)
	})
}

func verifyECDSAWalletConnectSignature(pubBin, sig []byte, signedDataFn signedDataFunc) (*ecdsa.PublicKey, bool) {
	return verifyECDSASignature(pubBin, sig, signedDataFn, func(pub *ecdsa.PublicKey) neofscrypto.PublicKey {
		return (*neofsecdsa.PublicKeyWalletConnect)(pub)
	})
}

// VerifyTokenSignature verifies t signature.
func VerifyTokenSignature[T interface {
	Signature() (neofscrypto.Signature, bool)
	SignedData() []byte
}](t T) (*ecdsa.PublicKey, bool) {
	sig, ok := t.Signature()
	if !ok {
		return nil, false
	}
	var pub *ecdsa.PublicKey
	switch scheme := sig.Scheme(); scheme {
	default:
		return nil, false
	case neofscrypto.ECDSA_SHA512:
		if pub, ok = verifyECDSASHA512Signature(sig.PublicKeyBytes(), sig.Value(), t.SignedData); !ok {
			return nil, false
		}
	case neofscrypto.ECDSA_DETERMINISTIC_SHA256:
		if pub, ok = verifyECDSARFC6979Signature(sig.PublicKeyBytes(), sig.Value(), t.SignedData); !ok {
			return nil, false
		}
	case neofscrypto.ECDSA_WALLETCONNECT:
		if pub, ok = verifyECDSAWalletConnectSignature(sig.PublicKeyBytes(), sig.Value(), t.SignedData); !ok {
			return nil, false
		}
	}
	return pub, true
}
