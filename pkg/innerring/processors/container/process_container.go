package container

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/meta"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"go.uber.org/zap"
)

func (cp *Processor) processCreateContainerRequest(req containerEvent.CreateContainerV2Request) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore container creation request")
		return
	}

	cnr, err := cntClient.ContainerFromStruct(req.Container)
	if err != nil {
		cp.log.Error("invalid container struct in creation request", zap.Error(err))
		return
	}

	cnrBytes := cnr.Marshal()
	id := cid.NewFromMarshalledContainer(cnrBytes)

	err = cp.checkPutContainer(cnr, cnrBytes, req.SessionToken, req.InvocationScript, req.VerificationScript, "", "")
	if err != nil {
		cp.log.Error("container creation request failed check",
			zap.Stringer("container", id), zap.Error(err))
		return
	}

	cp.approvePutContainer(req.MainTransaction, cnr, id)
}

// putEvent is a common interface of Put and PutNamed event.
type putEvent interface {
	event.Event
	Container() []byte
	PublicKey() []byte
	Signature() []byte
	SessionToken() []byte
	NotaryRequest() *payload.P2PNotaryRequest
}

// Process a new container from the user by checking the container sanity
// and sending approve tx back to the FS chain.
func (cp *Processor) processContainerPut(req containerEvent.CreateContainerRequest, id cid.ID) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore container put")
		return
	}

	var cnr containerSDK.Container
	if err := cnr.Unmarshal(req.Container); err != nil {
		cp.log.Error("put container check failed",
			zap.Error(fmt.Errorf("invalid binary container: %w", err)),
		)
		return
	}

	err := cp.checkPutContainer(cnr, req.Container, req.SessionToken, req.InvocationScript, req.VerificationScript, req.DomainName, req.DomainZone)
	if err != nil {
		cp.log.Error("put container check failed",
			zap.Error(err),
		)

		return
	}

	cp.approvePutContainer(req.MainTransaction, cnr, id)
}

const (
	sysAttrPrefix    = "__NEOFS__"
	sysAttrChainMeta = sysAttrPrefix + "METAINFO_CONSISTENCY"
)

var allowedSystemAttributes = map[string]struct{}{
	sysAttrPrefix + "NAME":                        {},
	sysAttrPrefix + "ZONE":                        {},
	sysAttrPrefix + "DISABLE_HOMOMORPHIC_HASHING": {},
	sysAttrPrefix + "LOCK_UNTIL":                  {},
	sysAttrChainMeta:                              {},
}

func (cp *Processor) checkPutContainer(cnr containerSDK.Container, cnrBytes, sessionToken, invocScript, verifScript []byte, domainName, domainZone string) error {
	for k := range cnr.Attributes() {
		if strings.HasPrefix(k, sysAttrPrefix) {
			if _, ok := allowedSystemAttributes[k]; !ok {
				return fmt.Errorf("system attribute %s is not allowed", k)
			}

			if k == sysAttrChainMeta && !cp.metaEnabled {
				return errors.New("chain meta data attribute is not allowed")
			}
		}
	}

	ecRules := cnr.PlacementPolicy().ECRules()
	if !cp.allowEC && len(ecRules) > 0 {
		return errors.New("EC rules are not supported yet")
	}
	if len(ecRules) > 0 && cnr.PlacementPolicy().NumberOfReplicas() > 0 {
		return errors.New("REP+EC rules are not supported yet")
	}

	err := cp.verifySignature(signatureVerificationData{
		ownerContainer:  cnr.Owner(),
		verb:            session.VerbContainerPut,
		verbV2:          sessionv2.VerbContainerPut,
		binTokenSession: sessionToken,
		verifScript:     verifScript,
		invocScript:     invocScript,
		signedData:      cnrBytes,
	})
	if err != nil {
		return fmt.Errorf("auth container creation: %w", err)
	}

	if err = cnr.PlacementPolicy().Verify(); err != nil {
		return fmt.Errorf("invalid storage policy: %w", err)
	}

	// check homomorphic hashing setting
	err = checkHomomorphicHashing(cp.netState, cnr)
	if err != nil {
		return fmt.Errorf("incorrect homomorphic hashing setting: %w", err)
	}

	if domainZone != "" { // if PutNamed event => check if values in-container domain name and zone correspond to args
		err = checkNNS(cnr, domainName, domainZone)
		if err != nil {
			return fmt.Errorf("NNS: %w", err)
		}
	}

	return nil
}

func (cp *Processor) approvePutContainer(mainTx transaction.Transaction, cnr containerSDK.Container, id cid.ID) {
	l := cp.log.With(zap.Stringer("cID", id))
	l.Debug("approving new container...")

	var err error

	err = cp.cnrClient.Morph().NotarySignAndInvokeTX(&mainTx, true)

	if err != nil {
		l.Error("could not approve put container",
			zap.Error(err),
		)
		return
	}

	nm, err := cp.netState.NetMap()
	if err != nil {
		l.Error("could not get netmap for Container contract update", zap.Error(err))
		return
	}

	policy := cnr.PlacementPolicy()
	vectors, err := nm.ContainerNodes(policy, id)
	if err != nil {
		l.Error("could not build placement for Container contract update", zap.Error(err))
		return
	}

	repRuleNum := policy.NumberOfReplicas()
	replicas := make([]uint32, len(vectors))
	for i := range repRuleNum {
		replicas[i] = policy.ReplicaNumberByIndex(i)
	}
	for i := repRuleNum; i < len(vectors); i++ { // EC rules
		replicas[i] = 1 // each EC part is stored in a single copy
	}

	err = cp.cnrClient.UpdateContainerPlacement(id, vectors, replicas)
	if err != nil {
		l.Error("could not update Container contract", zap.Error(err))
		return
	}

	if cp.metaEnabled {
		var metaPolicy string
		for k, v := range cnr.Attributes() {
			if k == sysAttrChainMeta && (v == "optimistic" || v == "strict") {
				metaPolicy = v
				break
			}
		}
		if metaPolicy != "" {
			l.Debug("sending registration transaction for metadata container...", zap.Stringer("cid", id), zap.String("metaPolicy", metaPolicy))

			_, err = cp.metaClient.WaitSuccess(cp.metaClient.Notarize(cp.metaClient.MakeTunedCall(meta.Hash, "registerMetaContainer", nil, func(r *result.Invoke, t *transaction.Transaction) error {
				if r.State != vmstate.Halt.String() {
					return fmt.Errorf("script failed (%s state) due to an error: %s", r.State, r.FaultException)
				}

				vub, err := processors.CalculateVUB(cp.metaClient)
				if err != nil {
					return fmt.Errorf("could not calculate vub: %w", err)
				}

				t.ValidUntilBlock = vub
				t.Nonce = mainTx.Nonce

				// Add 10% GAS to prevent this errors:
				// "at instruction 1689 (SYSCALL): System.Runtime.Log failed: insufficient amount of gas"
				t.SystemFee += t.SystemFee / 10

				return nil
			}, id[:])))
			if err != nil {
				l.Error("could not register meta container", zap.Stringer("cid", id), zap.Error(err))
				return
			}

			l.Debug("registered meta container", zap.Stringer("cid", id), zap.String("polices", metaPolicy))

			l.Debug("sending placement update transaction for metadata container...", zap.Stringer("cid", id), zap.String("metaPolicy", metaPolicy))

			err := processors.UpdateMetaPlacement(cp.metaClient, id, vectors, policy, mainTx.Nonce)
			if err != nil {
				l.Error("could not update placement", zap.Stringer("cid", id), zap.Error(err))
				return
			}

			l.Debug("updated meta container placement successfully...", zap.Stringer("cid", id), zap.String("metaPolicy", metaPolicy))
		}
	}

	l.Debug("container successfully approved")
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

	err = cp.verifySignature(signatureVerificationData{
		ownerContainer:  cnr.Owner(),
		verb:            session.VerbContainerDelete,
		verbV2:          sessionv2.VerbContainerDelete,
		idContainerSet:  true,
		idContainer:     idCnr,
		verifScript:     req.VerificationScript,
		binTokenSession: req.SessionToken,
		invocScript:     req.InvocationScript,
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

func (cp *Processor) processSetAttributeRequest(req containerEvent.SetAttributeRequest) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore attribute setting")
		return
	}

	id, err := cid.DecodeBytes(req.ID)
	if err != nil {
		cp.log.Error("attribute setting check failed",
			zap.Error(fmt.Errorf("invalid container ID: %w", err)))
		return
	}
	if id.IsZero() {
		cp.log.Error("attribute setting check failed",
			zap.Error(cid.ErrZero))
		return
	}

	err = cp.checkSetAttributeRequest(req, id)
	if err != nil {
		cp.log.Error("attribute setting check failed",
			zap.Stringer("container", id), zap.Error(err))
		return
	}

	cp.approveSetAttributeRequest(req, id)
}

func (cp *Processor) checkSetAttributeRequest(req containerEvent.SetAttributeRequest, id cid.ID) error {
	now := time.Now()
	if nowUnix := now.Unix(); nowUnix > req.ValidUntil {
		return fmt.Errorf("request is valid until %d (%s), now %d (%s)", req.ValidUntil, time.Unix(req.ValidUntil, 0).UTC(), nowUnix, now.UTC())
	}

	cnr, err := cp.cnrClient.Get(req.ID)
	if err != nil {
		return fmt.Errorf("get container by ID: %w", err)
	}

	signedData := client.GetSignedSetContainerAttributeParameters(client.SetContainerAttributeParameters{
		ID:         id,
		Attribute:  req.Attribute,
		Value:      req.Value,
		ValidUntil: time.Unix(req.ValidUntil, 0),
	})

	err = cp.verifySignature(signatureVerificationData{
		ownerContainer:  cnr.Owner(),
		verb:            session.VerbContainerSetAttribute,
		verbV2:          sessionv2.VerbContainerSetAttribute,
		idContainerSet:  true,
		idContainer:     id,
		verifScript:     req.VerificationScript,
		binTokenSession: req.SessionToken,
		invocScript:     req.InvocationScript,
		signedData:      signedData,
	})
	if err != nil {
		return fmt.Errorf("authenticate client: %w", err)
	}

	return nil
}

func (cp *Processor) approveSetAttributeRequest(req containerEvent.SetAttributeRequest, id cid.ID) {
	err := cp.cnrClient.Morph().NotarySignAndInvokeTX(&req.MainTransaction, false)
	if err != nil {
		cp.log.Error("could not approve attribute setting",
			zap.Stringer("container", id), zap.Error(err))
	}
}

func (cp *Processor) processRemoveAttributeRequest(req containerEvent.RemoveAttributeRequest) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore attribute removal")
		return
	}

	id, err := cid.DecodeBytes(req.ID)
	if err != nil {
		cp.log.Error("attribute removal check failed",
			zap.Error(fmt.Errorf("invalid container ID: %w", err)))
		return
	}
	if id.IsZero() {
		cp.log.Error("attribute removal check failed",
			zap.Error(cid.ErrZero))
		return
	}

	err = cp.checkRemoveAttributeRequest(req, id)
	if err != nil {
		cp.log.Error("attribute removal check failed",
			zap.Stringer("container", id), zap.Error(err))
		return
	}

	cp.approveRemoveAttributeRequest(req, id)
}

func (cp *Processor) checkRemoveAttributeRequest(req containerEvent.RemoveAttributeRequest, id cid.ID) error {
	now := time.Now()
	if nowUnix := now.Unix(); nowUnix > req.ValidUntil {
		return fmt.Errorf("request is valid until %d (%s), now %d (%s)", req.ValidUntil, time.Unix(req.ValidUntil, 0), nowUnix, now)
	}

	cnr, err := cp.cnrClient.Get(req.ID)
	if err != nil {
		return fmt.Errorf("get container by ID: %w", err)
	}

	signedData := client.GetSignedRemoveContainerAttributeParameters(client.RemoveContainerAttributeParameters{
		ID:         id,
		Attribute:  req.Attribute,
		ValidUntil: time.Unix(req.ValidUntil, 0),
	})

	err = cp.verifySignature(signatureVerificationData{
		ownerContainer:  cnr.Owner(),
		verb:            session.VerbContainerRemoveAttribute,
		verbV2:          sessionv2.VerbContainerRemoveAttribute,
		idContainerSet:  true,
		idContainer:     id,
		verifScript:     req.VerificationScript,
		binTokenSession: req.SessionToken,
		invocScript:     req.InvocationScript,
		signedData:      signedData,
	})
	if err != nil {
		return fmt.Errorf("authenticate client: %w", err)
	}

	return nil
}

func (cp *Processor) approveRemoveAttributeRequest(req containerEvent.RemoveAttributeRequest, id cid.ID) {
	err := cp.cnrClient.Morph().NotarySignAndInvokeTX(&req.MainTransaction, false)
	if err != nil {
		cp.log.Error("could not approve attribute removal",
			zap.Stringer("container", id), zap.Error(err))
	}
}

func checkNNS(cnr containerSDK.Container, name, zone string) error {
	// fetch domain info
	d := cnr.ReadDomain()

	if name != d.Name() {
		return fmt.Errorf("names differ %s/%s", name, d.Name())
	}

	if zone != d.Zone() {
		return fmt.Errorf("zones differ %s/%s", zone, d.Zone())
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
