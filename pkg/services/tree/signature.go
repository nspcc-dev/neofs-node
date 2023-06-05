package tree

import (
	"bytes"
	"crypto/ecdsa"
	"errors"
	"fmt"

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
	"go.uber.org/zap"
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

var errBearerWrongOwner = errors.New("bearer token must be signed by the container owner")
var errBearerWrongContainer = errors.New("bearer token is created for another container")
var errBearerSignature = errors.New("invalid bearer token signature")

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

	var tableFromBearer bool
	if len(rawBearer) != 0 {
		if !basicACL.AllowedBearerRules(op) {
			s.log.Debug("bearer presented but not allowed by ACL",
				zap.String("cid", cid.EncodeToString()),
				zap.String("op", op.String()),
			)
		} else {
			tableFromBearer = true
		}
	}

	var tb eacl.Table
	if tableFromBearer {
		var bt bearer.Token
		if err = bt.Unmarshal(rawBearer); err != nil {
			return eACLErr(eaclOp, fmt.Errorf("invalid bearer token: %w", err))
		}
		if !bearer.ResolveIssuer(bt).Equals(cnr.Value.Owner()) {
			return eACLErr(eaclOp, errBearerWrongOwner)
		}
		if !bt.AssertContainer(cid) {
			return eACLErr(eaclOp, errBearerWrongContainer)
		}
		if !bt.VerifySignature() {
			return eACLErr(eaclOp, errBearerSignature)
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

	return checkEACL(tb, req.GetSignature().GetKey(), eACLRole(role), eaclOp)
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

// SignMessage uses the provided key and signs any protobuf
// message that was generated for the TreeService by the
// protoc-gen-go-neofs generator. Returns any errors directly.
func SignMessage(m message, key *ecdsa.PrivateKey) error {
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

	var reqSigner user.ID
	err := user.IDFromKey(&reqSigner, req.GetSignature().GetKey())
	if err != nil {
		return role, fmt.Errorf("invalid public key: %w", err)
	}

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

var errDENY = errors.New("DENY eACL rule")
var errNoAllowRules = errors.New("not found allowing rules for the request")

// checkEACL searches for the eACL rules that could be applied to the request
// (a tuple of a signer key, his NeoFS role and a request operation).
// It does not filter the request by the filters of the eACL table since tree
// requests do not contain any "object" information that could be filtered and,
// therefore, filtering leads to unexpected results.
// The code was copied with the minor updates from the SDK repo:
// https://github.com/nspcc-dev/neofs-sdk-go/blob/43a57d42dd50dc60465bfd3482f7f12bcfcf3411/eacl/validator.go#L28.
func checkEACL(tb eacl.Table, signer []byte, role eacl.Role, op eacl.Operation) error {
	for _, record := range tb.Records() {
		// check type of operation
		if record.Operation() != op {
			continue
		}

		// check target
		if !targetMatches(record, role, signer) {
			continue
		}

		switch a := record.Action(); a {
		case eacl.ActionAllow:
			return nil
		case eacl.ActionDeny:
			return eACLErr(op, errDENY)
		default:
			return eACLErr(op, fmt.Errorf("unexpected action: %s", a))
		}
	}

	return eACLErr(op, errNoAllowRules)
}

func targetMatches(rec eacl.Record, role eacl.Role, signer []byte) bool {
	for _, target := range rec.Targets() {
		// check public key match
		if pubs := target.BinaryKeys(); len(pubs) != 0 {
			for _, key := range pubs {
				if bytes.Equal(key, signer) {
					return true
				}
			}

			continue
		}

		// check target group match
		if role == target.Role() {
			return true
		}
	}

	return false
}
