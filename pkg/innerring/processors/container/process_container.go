package container

import (
	"fmt"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
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
	e containerEvent.CreateContainerRequest

	// must be filled when verifying raw data from e
	cID cid.ID
	cnr containerSDK.Container
	d   containerSDK.Domain
}

// Process a new container from the user by checking the container sanity
// and sending approve tx back to the FS chain.
func (cp *Processor) processContainerPut(req containerEvent.CreateContainerRequest) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore container put")
		return
	}

	ctx := &putContainerContext{
		e: req,
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

const (
	sysAttrPrefix    = "__NEOFS__"
	sysAttrChainMeta = sysAttrPrefix + "METAINFO_CONSISTENCY"
)

var allowedSystemAttributes = map[string]struct{}{
	sysAttrPrefix + "NAME":                        {},
	sysAttrPrefix + "ZONE":                        {},
	sysAttrPrefix + "DISABLE_HOMOMORPHIC_HASHING": {},
	sysAttrChainMeta:                              {},
}

func (cp *Processor) checkPutContainer(ctx *putContainerContext) error {
	binCnr := ctx.e.Container
	ctx.cID = cid.NewFromMarshalledContainer(binCnr)

	err := ctx.cnr.Unmarshal(binCnr)
	if err != nil {
		return fmt.Errorf("invalid binary container: %w", err)
	}

	for k := range ctx.cnr.Attributes() {
		if strings.HasPrefix(k, sysAttrPrefix) {
			if _, ok := allowedSystemAttributes[k]; !ok {
				return fmt.Errorf("system attribute %s is not allowed", k)
			}

			if k == sysAttrChainMeta && !cp.metaEnabled {
				return fmt.Errorf("chain meta data attribute is not allowed")
			}
		}
	}

	err = cp.verifySignature(signatureVerificationData{
		ownerContainer:  ctx.cnr.Owner(),
		verb:            session.VerbContainerPut,
		binTokenSession: ctx.e.SessionToken,
		binPublicKey:    ctx.e.VerificationScript,
		signature:       ctx.e.InvocationScript,
		signedData:      binCnr,
	})
	if err != nil {
		return fmt.Errorf("auth container creation: %w", err)
	}

	if err = ctx.cnr.PlacementPolicy().Verify(); err != nil {
		return fmt.Errorf("invalid storage policy: %w", err)
	}

	// check homomorphic hashing setting
	err = checkHomomorphicHashing(cp.netState, ctx.cnr)
	if err != nil {
		return fmt.Errorf("incorrect homomorphic hashing setting: %w", err)
	}

	// check native name and zone
	err = checkNNS(ctx, ctx.cnr)
	if err != nil {
		return fmt.Errorf("NNS: %w", err)
	}

	return nil
}

func (cp *Processor) approvePutContainer(ctx *putContainerContext) {
	e := ctx.e

	var err error

	err = cp.cnrClient.Morph().NotarySignAndInvokeTX(&e.MainTransaction, true)

	if err != nil {
		cp.log.Error("could not approve put container",
			zap.Error(err),
		)
		return
	}

	nm, err := cp.netState.NetMap()
	if err != nil {
		cp.log.Error("could not get netmap for Container contract update", zap.Stringer("cid", ctx.cID), zap.Error(err))
		return
	}

	policy := ctx.cnr.PlacementPolicy()
	vectors, err := nm.ContainerNodes(policy, ctx.cID)
	if err != nil {
		cp.log.Error("could not build placement for Container contract update", zap.Stringer("cid", ctx.cID), zap.Error(err))
		return
	}

	replicas := make([]uint32, 0, policy.NumberOfReplicas())
	for i := range vectors {
		replicas = append(replicas, policy.ReplicaNumberByIndex(i))
	}

	err = cp.cnrClient.UpdateContainerPlacement(ctx.cID, vectors, replicas)
	if err != nil {
		cp.log.Error("could not update Container contract", zap.Stringer("cid", ctx.cID), zap.Error(err))
		return
	}
}

// Process delete container operation from the user by checking container sanity
// and sending approve tx back to FS chain.
func (cp *Processor) processContainerDelete(e containerEvent.RemoveContainerRequest) {
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

func (cp *Processor) checkDeleteContainer(req containerEvent.RemoveContainerRequest) error {
	var idCnr cid.ID

	err := idCnr.Decode(req.ID)
	if err != nil {
		return fmt.Errorf("invalid container ID: %w", err)
	}

	// receive owner of the related container
	cnr, err := cp.cnrClient.Get(req.ID)
	if err != nil {
		return fmt.Errorf("could not receive the container: %w", err)
	}

	if req.VerificationScript == nil { // 'delete' case
		req.VerificationScript = cnr.Signature.PublicKeyBytes()
	}

	err = cp.verifySignature(signatureVerificationData{
		ownerContainer:  cnr.Value.Owner(),
		verb:            session.VerbContainerDelete,
		idContainerSet:  true,
		idContainer:     idCnr,
		binPublicKey:    req.VerificationScript,
		binTokenSession: req.SessionToken,
		signature:       req.InvocationScript,
		signedData:      req.ID,
	})
	if err != nil {
		return fmt.Errorf("auth container removal: %w", err)
	}

	return nil
}

func (cp *Processor) approveDeleteContainer(e containerEvent.RemoveContainerRequest) {
	err := cp.cnrClient.Morph().NotarySignAndInvokeTX(&e.MainTransaction, false)

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
	if ctx.e.DomainName != "" {
		if ctx.e.DomainName != ctx.d.Name() {
			return fmt.Errorf("names differ %s/%s", ctx.e.DomainName, ctx.d.Name())
		}

		if ctx.e.DomainZone != ctx.d.Zone() {
			return fmt.Errorf("zones differ %s/%s", ctx.e.DomainZone, ctx.d.Zone())
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
