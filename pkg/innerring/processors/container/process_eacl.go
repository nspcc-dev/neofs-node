package container

import (
	"errors"
	"fmt"
	"math/big"

	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	cnrevent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"go.uber.org/zap"
)

func (cp *Processor) processPutEACLRequest(req cnrevent.PutContainerEACLRequest) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore set EACL")
		return
	}

	var (
		err   error
		table eacl.Table
		cnr   container.Container
	)
	defer func() {
		if err != nil {
			cp.log.Error("set EACL check failed",
				zap.Error(err),
			)
		}
	}()

	table, err = eacl.Unmarshal(req.EACL)
	if err != nil {
		// defer
		return
	}
	idCnr := table.GetCID()
	if idCnr.IsZero() {
		// defer
		err = errors.New("missing container ID in eACL table")
		return
	}
	cnr, err = cntClient.Get(cp.cnrClient, idCnr)
	if err != nil {
		// defer
		return
	}

	err = cp.checkSetEACL(req, table, cid.NewFromMarshalledContainer(cnr.Marshal()), cnr)
	if err != nil {
		// defer
		return
	}

	cp.approveSetEACL(req)
}

func (cp *Processor) checkSetEACL(req cnrevent.PutContainerEACLRequest, table eacl.Table, cID cid.ID, cnr container.Container) error {
	err := validateEACL(table)
	if err != nil {
		return fmt.Errorf("table validation: %w", err)
	}

	// ACL extensions can be disabled by basic ACL, check it
	if !cnr.BasicACL().Extendable() {
		return errors.New("ACL extension disabled by container basic ACL")
	}

	err = cp.verifySignature(signatureVerificationData{
		ownerContainer:  cnr.Owner(),
		verb:            session.VerbContainerSetEACL,
		verbV2:          sessionv2.VerbContainerSetEACL,
		idContainerSet:  true,
		idContainer:     cID,
		binTokenSession: req.SessionToken,
		verifScript:     req.VerificationScript,
		invocScript:     req.InvocationScript,
		signedData:      req.EACL,
	})
	if err != nil {
		return fmt.Errorf("auth eACL table setting: %w", err)
	}

	return nil
}

func (cp *Processor) approveSetEACL(req cnrevent.PutContainerEACLRequest) {
	err := cp.cnrClient.Morph().NotarySignAndInvokeTX(&req.MainTransaction, false)

	if err != nil {
		cp.log.Error("could not approve set EACL",
			zap.Error(err),
		)
	}
}

func validateEACL(t eacl.Table) error {
	var b big.Int
	for _, record := range t.Records() {
		for _, target := range record.Targets() {
			if target.Role() == eacl.RoleSystem {
				return errors.New("it is prohibited to modify system access")
			}
		}
		for _, f := range record.Filters() {
			//nolint:exhaustive
			switch f.Matcher() {
			case eacl.MatchNotPresent:
				if len(f.Value()) != 0 {
					return errors.New("non-empty value in absence filter")
				}
			case eacl.MatchNumGT, eacl.MatchNumGE, eacl.MatchNumLT, eacl.MatchNumLE:
				_, ok := b.SetString(f.Value(), 10)
				if !ok {
					return errors.New("numeric filter with non-decimal value")
				}
			}
		}
	}

	return nil
}
