package container

import (
	"errors"
	"fmt"

	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/session"
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
	binTable := e.Table()

	// unmarshal table
	table := eacl.NewTable()

	err := table.Unmarshal(binTable)
	if err != nil {
		return fmt.Errorf("invalid binary table: %w", err)
	}

	idCnr, ok := table.CID()
	if !ok {
		return errors.New("missing container ID in eACL table")
	}

	// receive owner of the related container
	cnr, err := cntClient.Get(cp.cnrClient, idCnr)
	if err != nil {
		return fmt.Errorf("could not receive the container: %w", err)
	}

	ownerContainer := cnr.OwnerID()
	if ownerContainer == nil {
		return errors.New("missing container owner")
	}

	err = cp.verifySignature(signatureVerificationData{
		ownerContainer:  *ownerContainer,
		verb:            session.VerbContainerSetEACL,
		idContainerSet:  true,
		idContainer:     idCnr,
		binTokenSession: e.SessionToken(),
		binPublicKey:    e.PublicKey(),
		signature:       e.Signature(),
		signedData:      binTable,
	})
	if err != nil {
		return fmt.Errorf("auth eACL table setting: %w", err)
	}

	return nil
}

func (cp *Processor) approveSetEACL(e container.SetEACL) {
	var err error

	prm := cntClient.PutEACLPrm{}

	prm.SetTable(e.Table())
	prm.SetKey(e.PublicKey())
	prm.SetSignature(e.Signature())
	prm.SetToken(e.SessionToken())

	if nr := e.NotaryRequest(); nr != nil {
		// setEACL event was received via Notary service
		err = cp.cnrClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)
	} else {
		// setEACL event was received via notification service
		err = cp.cnrClient.PutEACL(prm)
	}
	if err != nil {
		cp.log.Error("could not approve set EACL",
			zap.String("error", err.Error()),
		)
	}
}
