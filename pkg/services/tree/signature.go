package tree

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	core "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
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

func basicACLErr(op acl.Op) error {
	return fmt.Errorf("access to operation %s is denied by basic ACL check", op)
}

func eACLErr(op eacl.Operation, err error) error {
	return fmt.Errorf("access to operation %s is denied by extended ACL check: %w", op, err)
}

// verifyClient verifies if the request for a client operation
// was signed by a key allowed by (e)ACL rules.
// Operation must be one of:
//   - 1. ObjectPut;
//   - 2. ObjectGet.
func (s *Service) verifyClient(req message, cid cidSDK.ID, rawBearer []byte, op acl.Op) error {
	err := verifyMessage(req)
	if err != nil {
		return err
	}

	cnr, err := s.cnrSource.Get(cid)
	if err != nil {
		return fmt.Errorf("can't get container %s: %w", cid, err)
	}

	role, err := roleFromReq(cnr, req)
	if err != nil {
		return fmt.Errorf("can't get request role: %w", err)
	}

	basicACL := cnr.Value.BasicACL()

	if !basicACL.IsOpAllowed(op, role) {
		return basicACLErr(op)
	}

	if !basicACL.Extendable() {
		return nil
	}

	eaclOp := eACLOp(op)

	var tb eacl.Table
	if len(rawBearer) != 0 && basicACL.AllowedBearerRules(op) {
		var bt bearer.Token
		if err = bt.Unmarshal(rawBearer); err != nil {
			return eACLErr(eaclOp, fmt.Errorf("invalid bearer token: %w", err))
		}
		if !bearer.ResolveIssuer(bt).Equals(cnr.Value.Owner()) {
			return eACLErr(eaclOp, errors.New("bearer token must be signed by the container owner"))
		}
		if !bt.AssertContainer(cid) {
			return eACLErr(eaclOp, errors.New("bearer token is created for another container"))
		}
		if !bt.VerifySignature() {
			return eACLErr(eaclOp, errors.New("invalid bearer token signature"))
		}

		tb = bt.EACLTable()
	} else {
		tbCore, err := s.eaclSource.GetEACL(cid)
		if err != nil {
			if client.IsErrEACLNotFound(err) {
				return nil
			}

			return fmt.Errorf("get eACL table: %w", err)
		}

		tb = *tbCore.Value
	}

	// The default action should be DENY.
	action, found := eacl.NewValidator().CalculateAction(new(eacl.ValidationUnit).
		WithEACLTable(&tb).
		WithContainerID(&cid).
		WithRole(eACLRole(role)).
		WithSenderKey(req.GetSignature().GetKey()).
		WithOperation(eaclOp))
	if !found {
		return eACLErr(eaclOp, errors.New("not found allowing rules for the request"))
	} else if action != eacl.ActionAllow {
		return eACLErr(eaclOp, errors.New("DENY eACL rule"))
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
	if err := sigSDK.ReadFromV2(sigV2); err != nil {
		return fmt.Errorf("can't read signature: %w", err)
	}

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

func roleFromReq(cnr *core.Container, req message) (acl.Role, error) {
	role := acl.RoleOthers
	owner := cnr.Value.Owner()

	pub, err := keys.NewPublicKeyFromBytes(req.GetSignature().GetKey(), elliptic.P256())
	if err != nil {
		return role, fmt.Errorf("invalid public key: %w", err)
	}

	var reqSigner user.ID
	user.IDFromKey(&reqSigner, (ecdsa.PublicKey)(*pub))

	if reqSigner.Equals(owner) {
		role = acl.RoleOwner
	}

	return role, nil
}

func eACLOp(op acl.Op) eacl.Operation {
	switch op {
	case acl.OpObjectGet:
		return eacl.OperationGet
	case acl.OpObjectPut:
		return eacl.OperationPut
	default:
		panic(fmt.Sprintf("unexpected tree service ACL operation: %s", op))
	}
}

func eACLRole(role acl.Role) eacl.Role {
	switch role {
	case acl.RoleOwner:
		return eacl.RoleUser
	case acl.RoleOthers:
		return eacl.RoleOthers
	default:
		panic(fmt.Sprintf("unexpected tree service ACL role: %s", role))
	}
}
