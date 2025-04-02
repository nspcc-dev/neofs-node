package crypto

import (
	"crypto/ecdsa"
	"fmt"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
)

type signedDataFunc = func() []byte

func verifySignature(sig neofscrypto.Signature, signedData signedDataFunc) (*ecdsa.PublicKey, error) {
	switch scheme := sig.Scheme(); scheme {
	default:
		return nil, fmt.Errorf("unsupported scheme %v", scheme)
	case neofscrypto.ECDSA_SHA512, neofscrypto.ECDSA_DETERMINISTIC_SHA256, neofscrypto.ECDSA_WALLETCONNECT:
		pub, err := verifyECDSAFns[scheme](sig.PublicKeyBytes(), sig.Value(), signedData)
		if err != nil {
			return nil, schemeError(scheme, err)
		}
		return pub, nil
	}
}
