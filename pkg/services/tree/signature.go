package tree

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type message interface {
	SignedDataSize() int
	ReadSignedData([]byte) ([]byte, error)
	GetSignature() *Signature
	SetSignature(*Signature)
}

// verifyClient verifies that the request for a client operation was either signed by owner
// or contains a valid bearer token.
func (s *Service) verifyClient(req message, cid cidSDK.ID, rawBearer []byte, op eacl.Operation) error {
	err := verifyMessage(req)
	if err != nil {
		return err
	}

	cnr, err := s.cnrSource.Get(cid)
	if err != nil {
		return fmt.Errorf("can't get container %s: %w", cid, err)
	}

	ownerID := cnr.Value.Owner()

	if len(rawBearer) == 0 { // must be signed by the owner
		// No error is expected because `VerifyDataWithSource` checks the signature.
		// However, we may use different algorithms in the future, thus this check.
		pub, err := keys.NewPublicKeyFromBytes(req.GetSignature().GetKey(), elliptic.P256())
		if err != nil {
			return fmt.Errorf("invalid public key: %w", err)
		}

		var actualID user.ID
		user.IDFromKey(&actualID, (ecdsa.PublicKey)(*pub))

		if !actualID.Equals(ownerID) {
			return errors.New("`Move` request must be signed by a container owner")
		}
		return nil
	}

	var bt bearer.Token
	if err := bt.Unmarshal(rawBearer); err != nil {
		return fmt.Errorf("invalid bearer token: %w", err)
	}
	if !bearer.ResolveIssuer(bt).Equals(ownerID) {
		return errors.New("bearer token must be signed by the container owner")
	}
	if !bt.AssertContainer(cid) {
		return errors.New("bearer token is created for another container")
	}
	if !bt.VerifySignature() {
		return errors.New("invalid bearer token signature")
	}

	tb := bt.EACLTable()

	// The default action should be DENY, so we use RoleOthers to allow token issuer
	// to restrict everyone not affected by the previous rules.
	// This can be simplified after nspcc-dev/neofs-sdk-go#243 .
	action, found := eacl.NewValidator().CalculateAction(new(eacl.ValidationUnit).
		WithEACLTable(&tb).
		WithContainerID(&cid).
		WithRole(eacl.RoleOthers).
		WithSenderKey(req.GetSignature().GetKey()).
		WithOperation(op))
	if !found || action != eacl.ActionAllow {
		return errors.New("operation denied by bearer eACL")
	}
	return nil
}

func verifyMessage(m message) error {
	binBody, err := m.ReadSignedData(nil)
	if err != nil {
		return fmt.Errorf("marshal request body: %w", err)
	}

	sig := m.GetSignature()

	// TODO(@cthulhu-rider): #1387 use Signature message from NeoFS API to avoid conversion
	var sigV2 refs.Signature
	sigV2.SetKey(sig.GetKey())
	sigV2.SetSign(sig.GetSign())
	sigV2.SetScheme(refs.ECDSA_SHA512)

	var sigSDK neofscrypto.Signature
	sigSDK.ReadFromV2(sigV2)

	if !sigSDK.Verify(binBody) {
		return errors.New("invalid signature")
	}
	return nil
}

func signMessage(m message, key *ecdsa.PrivateKey) error {
	binBody, err := m.ReadSignedData(nil)
	if err != nil {
		return err
	}

	keySDK := neofsecdsa.Signer(*key)
	data, err := keySDK.Sign(binBody)
	if err != nil {
		return err
	}

	rawPub := make([]byte, keySDK.Public().MaxEncodedSize())
	rawPub = rawPub[:keySDK.Public().Encode(rawPub)]
	m.SetSignature(&Signature{
		Key:  rawPub,
		Sign: data,
	})

	return nil
}
