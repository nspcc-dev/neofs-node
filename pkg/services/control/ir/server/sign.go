package control

import (
	"bytes"
	"crypto/ecdsa"
	"errors"
	"fmt"

	control "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
)

// SignedMessage is an interface of Control service message.
type SignedMessage interface {
	ReadSignedData([]byte) ([]byte, error)
	GetSignature() *control.Signature
	SetSignature(*control.Signature)
}

var errDisallowedKey = errors.New("key is not in the allowed list")

func (s *Server) isValidRequest(req SignedMessage) error {
	sign := req.GetSignature()
	if sign == nil {
		// TODO(@cthulhu-rider): #1387 use "const" error
		return errors.New("missing signature")
	}

	var (
		key     = sign.GetKey()
		allowed = false
	)

	// check if key is allowed
	for i := range s.allowedKeys {
		if allowed = bytes.Equal(s.allowedKeys[i], key); allowed {
			break
		}
	}

	if !allowed {
		return errDisallowedKey
	}

	// verify signature
	binBody, err := req.ReadSignedData(nil)
	if err != nil {
		return fmt.Errorf("marshal request body: %w", err)
	}

	var pubKey neofsecdsa.PublicKey

	err = pubKey.Decode(sign.GetKey())
	if err != nil {
		return fmt.Errorf("decode public key in signature: %w", err)
	}

	sig := neofscrypto.NewSignature(neofscrypto.ECDSA_SHA512, &pubKey, sign.GetSign())

	if !sig.Verify(binBody) {
		// TODO(@cthulhu-rider): #1387 use "const" error
		return errors.New("invalid signature")
	}

	return nil
}

// SignMessage signs Control service message with private key.
func SignMessage(key *ecdsa.PrivateKey, msg SignedMessage) error {
	binBody, err := msg.ReadSignedData(nil)
	if err != nil {
		return fmt.Errorf("marshal request body: %w", err)
	}

	var sig neofscrypto.Signature

	err = sig.Calculate(neofsecdsa.Signer(*key), binBody)
	if err != nil {
		return fmt.Errorf("calculate signature: %w", err)
	}

	var sigControl control.Signature
	sigControl.SetKey(sig.PublicKeyBytes())
	sigControl.SetSign(sig.Value())

	msg.SetSignature(&sigControl)

	return nil
}
