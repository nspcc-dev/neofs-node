package tree

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	ownerSDK "github.com/nspcc-dev/neofs-sdk-go/owner"
	bearerSDK "github.com/nspcc-dev/neofs-sdk-go/token"
	sigutil "github.com/nspcc-dev/neofs-sdk-go/util/signature"
)

type message interface {
	sigutil.DataSource
	GetSignature() *Signature
	SetSignature(*Signature)
}

// verifyClient verifies that the request for a client operation was either signed by owner
// or contains a valid bearer token.
func (s *Service) verifyClient(req message, cid *cidSDK.ID, rawBearer []byte, op eacl.Operation) error {
	err := sigutil.VerifyDataWithSource(req, func() ([]byte, []byte) {
		s := req.GetSignature()
		return s.GetKey(), s.GetSign()
	})
	if err != nil {
		return err
	}

	cnr, err := s.cnrSource.Get(cid)
	if err != nil {
		return fmt.Errorf("can't get container %s: %w", cid, err)
	}

	if len(rawBearer) == 0 { // must be signed by the owner
		// No error is expected because `VerifyDataWithSource` checks the signature.
		// However, we may use different algorithms in the future, thus this check.
		pub, err := keys.NewPublicKeyFromBytes(req.GetSignature().GetKey(), elliptic.P256())
		if err != nil {
			return fmt.Errorf("invalid public key: %w", err)
		}

		actualID := ownerSDK.NewIDFromPublicKey((*ecdsa.PublicKey)(pub))
		if !actualID.Equal(cnr.OwnerID()) {
			return errors.New("request must be signed by a container owner")
		}
		return nil
	}

	var bearer bearerSDK.BearerToken
	if err := bearer.Unmarshal(rawBearer); err != nil {
		return fmt.Errorf("invalid bearer token: %w", err)
	}
	if !bearer.OwnerID().Equal(cnr.OwnerID()) {
		return errors.New("bearer token must be signed by the container owner")
	}
	if bCid := bearer.EACLTable().CID(); bCid != nil && !bCid.Equal(cid) {
		return errors.New("bearer token is created for another container")
	}
	if err := bearer.VerifySignature(); err != nil {
		return fmt.Errorf("invalid bearer token signature: %w", err)
	}

	// The default action should be DENY, so we use RoleOthers to allow token issuer
	// to restrict everyone not affected by the previous rules.
	// This can be simplified after nspcc-dev/neofs-sdk-go#243 .
	action := eacl.NewValidator().CalculateAction(new(eacl.ValidationUnit).
		WithEACLTable(bearer.EACLTable()).
		WithContainerID(cid).
		WithRole(eacl.RoleOthers).
		WithSenderKey(req.GetSignature().GetKey()).
		WithOperation(op))
	if action != eacl.ActionAllow {
		return errors.New("operation denied by bearer eACL")
	}
	return nil
}

func signMessage(resp message, key *ecdsa.PrivateKey) error {
	return sigutil.SignDataWithHandler(key, resp, func(key, sign []byte) {
		resp.SetSignature(&Signature{
			Key:  key,
			Sign: sign,
		})
	})
}
