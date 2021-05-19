package container

import (
	"crypto/elliptic"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
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

	// TODO: check key ownership

	return nil
}

func (cp *Processor) approveSetEACL(e container.SetEACL) {
	err := cp.cnrClient.PutEACLBinary(e.Table(), e.PublicKey(), e.Signature())
	if err != nil {
		cp.log.Error("could not approve set EACL",
			zap.String("error", err.Error()),
		)
	}
}
