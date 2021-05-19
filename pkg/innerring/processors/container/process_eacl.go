package container

import (
	"crypto/elliptic"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"go.uber.org/zap"
)

func (cp *Processor) processSetEACL(e container.SetEACL) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore set EACL")
		return
	}

	err := cp.checkSetEACL(e)
	if err != nil {
		cp.log.Error("set EACL check failed",
			zap.String("error", err.Error()),
		)

		return
	}

	cp.approveSetEACL(e)
}

func (cp *Processor) checkSetEACL(e container.SetEACL) error {
	// verify signature
	key, err := keys.NewPublicKeyFromBytes(e.PublicKey(), elliptic.P256())
	if err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	table := e.Table()
	tableHash := sha256.Sum256(table)

	if !key.Verify(e.Signature(), tableHash[:]) {
		return errors.New("invalid signature")
	}

	// verify the identity of the container owner
	return cp.checkEACLOwnership(table, key)
}

func (cp *Processor) checkEACLOwnership(binTable []byte, key *keys.PublicKey) error {
	// unmarshal table
	table := eacl.NewTable()

	err := table.Unmarshal(binTable)
	if err != nil {
		return fmt.Errorf("invalid binary table: %w", err)
	}

	// receive owner of the related container
	cnr, err := cp.cnrClient.Get(table.CID())
	if err != nil {
		return fmt.Errorf("could not receive the container: %w", err)
	}

	ownerID := cnr.OwnerID()

	// check key ownership
	ownerKeys, err := cp.idClient.AccountKeys(ownerID)
	if err != nil {
		return fmt.Errorf("could not received owner keys %s: %w", ownerID, err)
	}

	for _, ownerKey := range ownerKeys {
		if ownerKey.Equal(key) {
			return nil
		}
	}

	return fmt.Errorf("key %s is not tied to the owner of the container", key)
}

func (cp *Processor) approveSetEACL(e container.SetEACL) {
	err := cp.cnrClient.PutEACLBinary(e.Table(), e.PublicKey(), e.Signature())
	if err != nil {
		cp.log.Error("could not approve set EACL",
			zap.String("error", err.Error()),
		)
	}
}
