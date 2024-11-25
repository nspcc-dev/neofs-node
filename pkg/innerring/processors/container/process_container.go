package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
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

	d containerSDK.Domain
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
			zap.Error(err),
		)

		return
	}

	cp.approvePutContainer(ctx)
}

func (cp *Processor) checkPutContainer(ctx *putContainerContext) error {
	binCnr := ctx.e.Container()
	var cnr containerSDK.Container

	err := cnr.Unmarshal(binCnr)
	if err != nil {
		return fmt.Errorf("invalid binary container: %w", err)
	}

	err = cp.verifySignature(signatureVerificationData{
		ownerContainer:  cnr.Owner(),
		verb:            session.VerbContainerPut,
		binTokenSession: ctx.e.SessionToken(),
		binPublicKey:    ctx.e.PublicKey(),
		signature:       ctx.e.Signature(),
		signedData:      binCnr,
	})
	if err != nil {
		return fmt.Errorf("auth container creation: %w", err)
	}

	// check homomorphic hashing setting
	err = checkHomomorphicHashing(cp.netState, cnr)
	if err != nil {
		return fmt.Errorf("incorrect homomorphic hashing setting: %w", err)
	}

	// check native name and zone
	err = checkNNS(ctx, cnr)
	if err != nil {
		return fmt.Errorf("NNS: %w", err)
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
	prm.SetName(ctx.d.Name())
	prm.SetZone(ctx.d.Zone())

	nr := e.NotaryRequest()
	err = cp.cnrClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)

	if err != nil {
		cp.log.Error("could not approve put container",
			zap.Error(err),
		)
	}
}

// Process delete container operation from the user by checking container sanity
// and sending approve tx back to morph.
func (cp *Processor) processContainerDelete(e *containerEvent.Delete) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore container delete")
		return
	}

	err := cp.checkDeleteContainer(e)
	if err != nil {
		cp.log.Error("delete container check failed",
			zap.Error(err),
		)

		return
	}

	cp.approveDeleteContainer(e)
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

	err = cp.verifySignature(signatureVerificationData{
		ownerContainer:  cnr.Value.Owner(),
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

	nr := e.NotaryRequest()
	err = cp.cnrClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)

	if err != nil {
		cp.log.Error("could not approve delete container",
			zap.Error(err),
		)
	}
}

func checkNNS(ctx *putContainerContext, cnr containerSDK.Container) error {
	// fetch domain info
	ctx.d = cnr.ReadDomain()

	// if PutNamed event => check if values in container correspond to args
	if named, ok := ctx.e.(interface {
		Name() string
		Zone() string
	}); ok {
		if name := named.Name(); name != ctx.d.Name() {
			return fmt.Errorf("names differ %s/%s", name, ctx.d.Name())
		}

		if zone := named.Zone(); zone != ctx.d.Zone() {
			return fmt.Errorf("zones differ %s/%s", zone, ctx.d.Zone())
		}
	}

	return nil
}

func checkHomomorphicHashing(ns NetworkState, cnr containerSDK.Container) error {
	netSetting, err := ns.HomomorphicHashDisabled()
	if err != nil {
		return fmt.Errorf("could not get setting in contract: %w", err)
	}

	if cnrSetting := cnr.IsHomomorphicHashingDisabled(); netSetting != cnrSetting {
		return fmt.Errorf("network setting: %t, container setting: %t", netSetting, cnrSetting)
	}

	return nil
}
