package control

import (
	"bytes"
	"crypto/ecdsa"
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/nspcc-dev/neofs-sdk-go/util/signature"
)

// SignedMessage is an interface of Control service message.
type SignedMessage interface {
	signature.DataSource
	GetSignature() *control.Signature
	SetSignature(*control.Signature)
}

var errDisallowedKey = errors.New("key is not in the allowed list")

func (s *Server) isValidRequest(req SignedMessage) error {
	var (
		sign    = req.GetSignature()
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
	return signature.VerifyDataWithSource(req, func() ([]byte, []byte) {
		return key, sign.GetSign()
	})
}

// SignMessage signs Control service message with private key.
func SignMessage(key *ecdsa.PrivateKey, msg SignedMessage) error {
	return signature.SignDataWithHandler(key, msg, func(key []byte, sig []byte) {
		s := new(control.Signature)
		s.SetKey(key)
		s.SetSign(sig)

		msg.SetSignature(s)
	})
}
