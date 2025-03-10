package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"
	"slices"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
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

func verifyN3Signature(verifScript, invocScript []byte, signedDataFn signedDataFunc) error {
	if len(verifScript) > 0 {
		// TODO: scripts may be stateless like simple- or multi-signature, they do not require exec on a VM
		iatEpoch := tok.Iat()
		iatHeight, err := interface {
			HeightAtEpoch(uint64) (uint32, error)
		}(nil).HeightAtEpoch(tok.Iat())
		if err != nil {
			return fmt.Errorf("get FS chain height (session iat: epoch#%d): %w", iatEpoch, err)
		}
		blkHdr, err := interface {
			GetBlockHeaderByIndex(uint32) (block.Header, error)
		}(nil).GetBlockHeaderByIndex(iatHeight)
		if err != nil {
			return fmt.Errorf("get FS chain block header (session iat: epoch#%d, height#%d): %w", iatHeight, iatEpoch, err)
		}
		// FIXME: add signed token data to the exec context
		tx := &transaction.Transaction{
			Script:  slices.Concat(invocScript, verifScript),
			Signers: []transaction.Signer{{Account: acc}},
			// FIXME: what else?
		}
		// w8 4 https://github.com/nspcc-dev/neo-go/issues/3836
		ok, err := unwrap.Bool(cp.cnrClient.Morph().InvokeContainedScript(tx, blkHdr, trigger.Verification))
		if err != nil {
			return fmt.Errorf("invoke contained auth script on FS chain (session iat: epoch#%d, height#%d): %w", iatHeight, iatEpoch, err)
		}
		if !ok {
			return fmt.Errorf("auth script run on FS chain resulted in false (session iat: epoch#%d, height#%d)", iatHeight, iatEpoch)
		}
	} else { // FIXME: what?
	}
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
	case 3: // TODO: use const after SDK upgrade
		if err := verifyN3Signature(sig.PublicKeyBytes(), sig.Value(), t.SignedData); err != nil {
			return nil, fmt.Errorf("%s scheme: %w", "N3", err) // TODO: use const after SDK upgrade
		}
	}
	return pub, nil
}
