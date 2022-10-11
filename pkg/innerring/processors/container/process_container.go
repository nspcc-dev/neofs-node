package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/models"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"go.uber.org/zap"
)

func (x *Processor) ProcessContainerCreation(req models.ContainerCreationRequest) bool {
	log := x.log.With(
		zap.Binary("container", req.Container),
		zap.Binary("public key", req.PublicKey),
		zap.Binary("signature", req.Signature),
		zap.Binary("session", req.Session),
		zap.String("domain name", req.Name),
		zap.String("domain zone", req.Zone),
	)

	log.Debug("container creation request received, processing...")

	isAlphabet, err := x.node.IsAlphabet()
	if err != nil {
		log.Error("failed to determine Alphabet status of the local node", zap.Error(err))
		return false
	} else if !isAlphabet {
		log.Info("local node is not an Alphabet one, skip processing")
		return false
	}

	err = x.checkPutContainer(req)
	if err != nil {
		log.Error("container creation check failed", zap.Error(err))
		return false
	}

	return true
}

func (x *Processor) checkPutContainer(req models.ContainerCreationRequest) error {
	var cnr containerSDK.Container

	err := cnr.Unmarshal(req.Container)
	if err != nil {
		return fmt.Errorf("invalid binary container: %w", err)
	}

	creator := cnr.Owner()

	err = x.verifySignature(signatureVerificationData{
		containerCreator: creator,
		verb:             session.VerbContainerPut,
		binTokenSession:  req.Session,
		binPublicKey:     req.PublicKey,
		signature:        req.Signature,
		signedData:       req.Container,
	})
	if err != nil {
		return fmt.Errorf("auth container creation: %w", err)
	}

	// check creator allowance in the subnetwork
	err = x.neoFS.CheckUserAllowanceToSubnet(cnr.PlacementPolicy().Subnet(), creator)
	if err != nil {
		return fmt.Errorf("check subnet access: %w", err)
	}

	// check homomorphic hashing setting
	err = checkHomomorphicHashing(x.neoFS, cnr)
	if err != nil {
		return fmt.Errorf("incorrect homomorphic hashing setting: %w", err)
	}

	// check native name and zone
	err = checkNNS(req, cnr)
	if err != nil {
		return fmt.Errorf("NNS: %w", err)
	}

	return nil
}

func (x *Processor) ProcessContainerRemoval(req models.ContainerRemovalRequest) bool {
	log := x.log.With(
		zap.Binary("container", req.Container),
		zap.Binary("signature", req.Signature),
		zap.Binary("session", req.Session),
	)

	log.Debug("container removal request received, processing...")

	isAlphabet, err := x.node.IsAlphabet()
	if err != nil {
		log.Error("failed to determine Alphabet status of the local node", zap.Error(err))
		return false
	} else if !isAlphabet {
		log.Info("local node is not an Alphabet one, skip processing")
		return false
	}

	err = x.checkDeleteContainer(req)
	if err != nil {
		log.Error("container removal check failed", zap.Error(err))
		return false
	}

	return true
}

func (x *Processor) checkDeleteContainer(req models.ContainerRemovalRequest) error {
	var idCnr cid.ID

	err := idCnr.Decode(req.Container)
	if err != nil {
		return fmt.Errorf("invalid container ID: %w", err)
	}

	creator, err := x.neoFS.ContainerCreator(idCnr)
	if err != nil {
		return fmt.Errorf("get container creator: %w", err)
	}

	err = x.verifySignature(signatureVerificationData{
		containerCreator: creator,
		verb:             session.VerbContainerDelete,
		idContainerSet:   true,
		idContainer:      idCnr,
		binTokenSession:  req.Session,
		signature:        req.Signature,
		signedData:       req.Container,
	})
	if err != nil {
		return fmt.Errorf("auth container removal: %w", err)
	}

	return nil
}

func checkNNS(req models.ContainerCreationRequest, cnr containerSDK.Container) error {
	// fetch domain info
	d := containerSDK.ReadDomain(cnr)

	if req.Name != "" {
		// if event with domain registration => check if values in container correspond to args
		if req.Name != d.Name() {
			return fmt.Errorf("names differ %s/%s", req.Name, d.Name())
		}

		if req.Zone != d.Zone() {
			return fmt.Errorf("zones differ %s/%s", req.Zone, d.Zone())
		}
	}

	return nil
}

func checkHomomorphicHashing(ns NeoFS, cnr containerSDK.Container) error {
	netSetting, err := ns.IsHomomorphicHashingDisabled()
	if err != nil {
		return fmt.Errorf("could not get setting in contract: %w", err)
	}

	if cnrSetting := containerSDK.IsHomomorphicHashingDisabled(cnr); netSetting != cnrSetting {
		return fmt.Errorf("network setting: %t, container setting: %t", netSetting, cnrSetting)
	}

	return nil
}
