package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
)

var verifyECDSAFns = map[neofscrypto.Scheme]func(_ ecdsa.PublicKey, sig, data []byte) bool{
	neofscrypto.ECDSA_SHA512:               verifyECDSASHA512Signature,
	neofscrypto.ECDSA_DETERMINISTIC_SHA256: verifyECDSARFC6979Signature,
	neofscrypto.ECDSA_WALLETCONNECT:        verifyECDSAWalletConnectSignature,
}

func decodeECDSAPublicKey(b []byte) (*ecdsa.PublicKey, error) {
	if len(b) > 0 && b[0] == 0x00 {
		return nil, errors.New("invalid prefix 0")
	}
	pub, err := keys.NewPublicKeyFromBytes(b, elliptic.P256())
	if err != nil {
		return nil, err
	}
	return (*ecdsa.PublicKey)(pub), nil
}

func verifyECDSASHA512Signature(pub ecdsa.PublicKey, sig, data []byte) bool {
	return (*neofsecdsa.PublicKey)(&pub).Verify(data, sig)
}

func verifyECDSARFC6979Signature(pub ecdsa.PublicKey, sig, data []byte) bool {
	return (*neofsecdsa.PublicKeyRFC6979)(&pub).Verify(data, sig)
}

func verifyECDSAWalletConnectSignature(pub ecdsa.PublicKey, sig, data []byte) bool {
	return (*neofsecdsa.PublicKeyWalletConnect)(&pub).Verify(data, sig)
}
