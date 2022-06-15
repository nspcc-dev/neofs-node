package container

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	morphsubnet "github.com/nspcc-dev/neofs-node/pkg/morph/client/subnet"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"go.uber.org/zap"
)

// putEvent is a common interface of Put and PutNamed event.
type putEvent interface {
	event.Event
	Container() []byte
	PublicKey() []byte
	Signature() []byte
	SessionToken() []byte
	NotaryRequest() *payload.P2PNotaryRequest
}

type putContainerContext struct {
	e putEvent

	name, zone string // from container structure
}

// Process a new container from the user by checking the container sanity
// and sending approve tx back to the morph.
func (cp *Processor) processContainerPut(put putEvent) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore container put")
		return
	}

	ctx := &putContainerContext{
		e: put,
	}

	err := cp.checkPutContainer(ctx)
	if err != nil {
		cp.log.Error("put container check failed",
			zap.String("error", err.Error()),
		)

		return
	}

	cp.approvePutContainer(ctx)
}

func (cp *Processor) checkPutContainer(ctx *putContainerContext) error {
	binCnr := ctx.e.Container()

	cnr := containerSDK.New()

	err := cnr.Unmarshal(binCnr)
	if err != nil {
		return fmt.Errorf("invalid binary container: %w", err)
	}

	ownerContainer := cnr.OwnerID()
	if ownerContainer == nil {
		return errors.New("missing container owner")
	}

	err = cp.verifySignature(signatureVerificationData{
		ownerContainer:  *ownerContainer,
		verb:            session.VerbContainerPut,
		binTokenSession: ctx.e.SessionToken(),
		binPublicKey:    ctx.e.PublicKey(),
		signature:       ctx.e.Signature(),
		signedData:      binCnr,
	})
	if err != nil {
		return fmt.Errorf("auth container creation: %w", err)
	}

	// check owner allowance in the subnetwork
	err = checkSubnet(cp.subnetClient, cnr)
	if err != nil {
		return fmt.Errorf("incorrect subnetwork: %w", err)
	}

	// check native name and zone
	err = checkNNS(ctx, cnr)
	if err != nil {
		return fmt.Errorf("NNS: %w", err)
	}

	// perform format check
	err = container.CheckFormat(cnr)
	if err != nil {
		return fmt.Errorf("incorrect container format: %w", err)
	}

	return nil
}

func (cp *Processor) approvePutContainer(ctx *putContainerContext) {
	e := ctx.e

	var err error

	prm := cntClient.PutPrm{}

	prm.SetContainer(e.Container())
	prm.SetKey(e.PublicKey())
	prm.SetSignature(e.Signature())
	prm.SetToken(e.SessionToken())
	prm.SetName(ctx.name)
	prm.SetZone(ctx.zone)

	if nr := e.NotaryRequest(); nr != nil {
		// put event was received via Notary service
		err = cp.cnrClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)
	} else {
		// put event was received via notification service
		err = cp.cnrClient.Put(prm)
	}
	if err != nil {
		cp.log.Error("could not approve put container",
			zap.String("error", err.Error()),
		)
	}
}

// Process delete container operation from the user by checking container sanity
// and sending approve tx back to morph.
func (cp *Processor) processContainerDelete(delete *containerEvent.Delete) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore container delete")
		return
	}

	err := cp.checkDeleteContainer(delete)
	if err != nil {
		cp.log.Error("delete container check failed",
			zap.String("error", err.Error()),
		)

		return
	}

	cp.approveDeleteContainer(delete)
}

func (cp *Processor) checkDeleteContainer(e *containerEvent.Delete) error {
	binCnr := e.ContainerID()

	var idCnr cid.ID

	err := idCnr.Decode(binCnr)
	if err != nil {
		return fmt.Errorf("invalid container ID: %w", err)
	}

	// receive owner of the related container
	cnr, err := cp.cnrClient.Get(binCnr)
	if err != nil {
		return fmt.Errorf("could not receive the container: %w", err)
	}

	ownerContainer := cnr.OwnerID()
	if ownerContainer == nil {
		return errors.New("missing container owner")
	}

	err = cp.verifySignature(signatureVerificationData{
		ownerContainer:  *ownerContainer,
		verb:            session.VerbContainerDelete,
		idContainerSet:  true,
		idContainer:     idCnr,
		binTokenSession: e.SessionToken(),
		signature:       e.Signature(),
		signedData:      binCnr,
	})
	if err != nil {
		return fmt.Errorf("auth container removal: %w", err)
	}

	return nil
}

func (cp *Processor) approveDeleteContainer(e *containerEvent.Delete) {
	var err error

	prm := cntClient.DeletePrm{}

	prm.SetCID(e.ContainerID())
	prm.SetSignature(e.Signature())
	prm.SetToken(e.SessionToken())

	if nr := e.NotaryRequest(); nr != nil {
		// delete event was received via Notary service
		err = cp.cnrClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)
	} else {
		// delete event was received via notification service
		err = cp.cnrClient.Delete(prm)
	}
	if err != nil {
		cp.log.Error("could not approve delete container",
			zap.String("error", err.Error()),
		)
	}
}

func checkNNS(ctx *putContainerContext, cnr *containerSDK.Container) error {
	// fetch native name and zone
	ctx.name, ctx.zone = containerSDK.GetNativeNameWithZone(cnr)

	// if PutNamed event => check if values in container correspond to args
	if named, ok := ctx.e.(interface {
		Name() string
		Zone() string
	}); ok {
		if name := named.Name(); name != ctx.name {
			return fmt.Errorf("names differ %s/%s", name, ctx.name)
		}

		if zone := named.Zone(); zone != ctx.zone {
			return fmt.Errorf("zones differ %s/%s", zone, ctx.zone)
		}
	}

	return nil
}

func checkSubnet(subCli *morphsubnet.Client, cnr *containerSDK.Container) error {
	owner := cnr.OwnerID()
	if owner == nil {
		return errors.New("missing owner")
	}

	prm := morphsubnet.UserAllowedPrm{}

	subID := cnr.PlacementPolicy().SubnetID()
	if subID == nil || subnetid.IsZero(*subID) {
		return nil
	}

	prm.SetID(subID.Marshal())
	prm.SetClient(owner.WalletBytes())

	res, err := subCli.UserAllowed(prm)
	if err != nil {
		return fmt.Errorf("could not check user in contract: %w", err)
	}

	if !res.Allowed() {
		return fmt.Errorf("user is not allowed to create containers in %s subnetwork", subID)
	}

	return nil
}
