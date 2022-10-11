package container

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/models"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"go.uber.org/zap"
)

func (x *Processor) ProcessSetContainerExtendedACLRequest(req models.SetContainerExtendedACLRequest) bool {
	log := x.log.With(
		zap.Binary("eACL", req.ExtendedACL),
		zap.Binary("public key", req.PublicKey),
		zap.Binary("signature", req.Signature),
		zap.Binary("session", req.Session),
	)

	log.Debug("set container eACL request received, processing...")

	isAlphabet, err := x.node.IsAlphabet()
	if err != nil {
		log.Error("failed to determine Alphabet status of the local node", zap.Error(err))
		return false
	} else if !isAlphabet {
		log.Info("local node is not an Alphabet one, skip processing")
		return false
	}

	err = x.checkSetEACL(req)
	if err != nil {
		x.log.Error("set container eACL check failed", zap.Error(err))
		return false
	}

	return true
}

func (x *Processor) checkSetEACL(req models.SetContainerExtendedACLRequest) error {
	// unmarshal table
	table := eacl.NewTable()

	err := table.Unmarshal(req.ExtendedACL)
	if err != nil {
		return fmt.Errorf("invalid binary eACL table: %w", err)
	}

	idCnr, ok := table.CID()
	if !ok {
		return errors.New("missing container ID in eACL table")
	}

	// ACL extensions can be disabled, check it
	extendable, err := x.neoFS.IsContainerACLExtendable(idCnr)
	if err != nil {
		return fmt.Errorf("check container eACL extendability: %w", err)
	} else if !extendable {
		return errors.New("container ACL is immutable")
	}

	creator, err := x.neoFS.ContainerCreator(idCnr)
	if err != nil {
		return fmt.Errorf("get container creator: %w", err)
	}

	err = x.verifySignature(signatureVerificationData{
		containerCreator: creator,
		verb:             session.VerbContainerSetEACL,
		idContainerSet:   true,
		idContainer:      idCnr,
		binTokenSession:  req.Session,
		binPublicKey:     req.PublicKey,
		signature:        req.Signature,
		signedData:       req.ExtendedACL,
	})
	if err != nil {
		return fmt.Errorf("auth eACL table setting: %w", err)
	}

	return nil
}
