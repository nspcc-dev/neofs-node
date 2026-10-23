package container

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	protocontainer "github.com/nspcc-dev/neofs-sdk-go/proto/container"
	protoencoding "github.com/nspcc-dev/neofs-sdk-go/proto/encoding"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"go.uber.org/zap"
)

var minContainerAPIVersion = version.New(2, 22)

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

	err = cp.checkPutContainer(cnr, cnrBytes, req.SessionToken, req.InvocationScript, req.VerificationScript, req.OriginalRequest)
	if err != nil {
		cp.log.Error("container creation request failed check",
			zap.Stringer("container", id), zap.Error(err))
		return
	}

	if req.EACLTable != nil {
		cp.log.Debug("additional eACL table setting with container creation", zap.Stringer("cid", id))

		var table eacl.Table
		err = table.Unmarshal(req.EACLTable.EACL)
		if err != nil {
			cp.log.Error("invalid eACL table in container put request", zap.Stringer("cid", id), zap.Error(err))
			return
		}
		if id != table.GetCID() {
			cp.log.Error("additional eACL table in container put request has different container",
				zap.Stringer("cid", id), zap.Stringer("cidInEACL", table.GetCID()))
			return
		}

		err = cp.checkSetEACL(*req.EACLTable, table, id, cnr, req.OriginalRequest == nil)
		if err != nil {
			cp.log.Error("additional eACL with container creation check failed",
				zap.Stringer("cid", id), zap.Error(err))
			return
		}

		// optional eACL setting operation is included in the full main
		// transaction, no need for any additional eACL approval there
	}

	cp.approvePutContainer(req.MainTransaction, cnr, id)
}

const (
	sysAttrPrefix    = "__NEOFS__"
	sysAttrChainMeta = sysAttrPrefix + "METAINFO_CONSISTENCY"
)

var allowedSystemAttributes = map[string]struct{}{
	sysAttrPrefix + "NAME":       {},
	sysAttrPrefix + "ZONE":       {},
	sysAttrPrefix + "LOCK_UNTIL": {},
	sysAttrChainMeta:             {},
}

func (cp *Processor) checkPutContainer(cnr container.Container, cnrBytes, sessionToken, invocScript, verifScript []byte, origReq []byte) error {
	if ver := cnr.Version(); ver.Compare(minContainerAPIVersion) < 0 {
		return fmt.Errorf("API version is %s while %s is minimum supported", ver, minContainerAPIVersion)
	}

	var metaEnabled bool
	for k := range cnr.Attributes() {
		if strings.HasPrefix(k, sysAttrPrefix) {
			if _, ok := allowedSystemAttributes[k]; !ok {
				return fmt.Errorf("system attribute %s is not allowed", k)
			}

			if k == sysAttrChainMeta {
				if !cp.metaEnabled {
					return errors.New("chain meta data attribute is not allowed")
				}
				metaEnabled = true
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
	if metaEnabled && cnr.PlacementPolicy().Initial() != nil {
		return errors.New("initial placement policies with consistent metadata are not supported yet")
	}

	if origReq != nil {
		var apiReq protocontainer.PutRequest
		err := protoencoding.UnmarshalMessage(origReq, &apiReq)
		if err != nil {
			return fmt.Errorf("could not unmarshal original request: %w", err)
		}

		var (
			body = apiReq.GetBody()
			mCnr = body.GetContainer()
		)
		if mCnr == nil || !bytes.Equal(cnrBytes, protoencoding.MarshalMessage(mCnr)) {
			return fmt.Errorf("container in the original request does not match the contract one")
		}

		return cp.checkOriginalRequest(cnr, cid.ID{}, session.VerbContainerPut, sessionv2.VerbContainerPut, body, apiReq.GetMetaHeader(), apiReq.GetVerifyHeader())
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

	return nil
}

func (cp *Processor) approvePutContainer(mainTx transaction.Transaction, cnr container.Container, id cid.ID) {
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

			err = cp.metaClient.RegisterMetadataContainer(id, mainTx.Nonce)
			if err != nil {
				l.Error("could not register meta container", zap.Stringer("cid", id), zap.Error(err))
				return
			}

			l.Debug("registered meta container", zap.Stringer("cid", id), zap.String("metaPolicy", metaPolicy))

			l.Debug("sending placement update transaction for metadata container...", zap.Stringer("cid", id), zap.String("metaPolicy", metaPolicy))

			err := cp.metaClient.UpdateContainerPlacement(id, vectors, policy, mainTx.Nonce)
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

	if req.OriginalRequest != nil {
		var origReq protocontainer.DeleteRequest
		err := protoencoding.UnmarshalMessage(req.OriginalRequest, &origReq)
		if err != nil {
			return fmt.Errorf("could not unmarshal original request: %w", err)
		}
		var body = origReq.GetBody()
		if !bytes.Equal(req.ID, body.GetContainerId().GetValue()) {
			return fmt.Errorf("container ID in the original request does not match the contract one")
		}

		return cp.checkOriginalRequest(cnr, idCnr, session.VerbContainerDelete, sessionv2.VerbContainerDelete, body, origReq.GetMetaHeader(), origReq.GetVerifyHeader())
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

func (cp *Processor) checkOriginalRequest(cnr container.Container, cnrID cid.ID, verbV1 session.ContainerVerb, verbV2 sessionv2.Verb, reqBody protoencoding.Message, reqMetaHeader *protosession.RequestMetaHeader, reqVerifHeader *protosession.RequestVerificationHeader) error {
	if err := util.VerifyRequestAPIVersion(reqMetaHeader.GetVersion()); err != nil {
		return fmt.Errorf("invalid request API version: %w", err)
	}
	if err := util.VerifyRequestValidityTime(cp.chainTime.Now(), reqMetaHeader); err != nil {
		return fmt.Errorf("invalid request validity time: %w", err)
	}

	var sTok []byte
	switch {
	case reqMetaHeader.GetSessionToken() != nil:
		t := reqMetaHeader.GetSessionToken()
		sTok = make([]byte, t.MarshaledSize())
		t.MarshalStable(sTok)
	case reqMetaHeader.GetSessionTokenV2() != nil:
		t := reqMetaHeader.GetSessionTokenV2()
		sTok = make([]byte, t.MarshaledSize())
		t.MarshalStable(sTok)
	default:
	}

	var (
		signedReq, _ = protoencoding.EncodeRequest(nil, reqBody, reqMetaHeader)
		sig          = reqVerifHeader.GetRequestSignature()
		verifData    = signatureVerificationData{
			ownerContainer:  cnr.Owner(),
			verb:            verbV1,
			verbV2:          verbV2,
			idContainerSet:  !cnrID.IsZero(),
			idContainer:     cnrID,
			verifScript:     sig.GetKey(),
			binTokenSession: sTok,
			invocScript:     sig.GetSign(),
			signedData:      signedReq,
		}
	)

	if multipleReqSignatures(reqMetaHeader) {
		// API 2.25- requests.

		var (
			bodyLen    = reqBody.MarshaledSize()
			mHeaderLen = reqMetaHeader.MarshaledSize()
			needBuff   = max(bodyLen, mHeaderLen)
			signedData = slices.Grow(verifData.signedData[:0], needBuff)
		)

		// body

		signedData = signedData[:bodyLen]
		reqBody.MarshalStable(signedData)

		sig := reqVerifHeader.GetBodySignature()

		verifData.signedData = signedData
		verifData.verifScript = sig.GetKey()
		verifData.invocScript = sig.GetSign()

		err := cp.verifySignature(verifData)
		if err != nil {
			return fmt.Errorf("body signature auth error: %w", err)
		}

		// meta header

		signedData = signedData[:mHeaderLen]
		reqMetaHeader.MarshalStable(signedData)

		sig = reqVerifHeader.GetMetaSignature()

		verifData.signedData = signedData
		verifData.verifScript = sig.GetKey()
		verifData.invocScript = sig.GetSign()

		err = cp.verifySignature(verifData)
		if err != nil {
			return fmt.Errorf("meta header auth error: %w", err)
		}

		return nil
	}

	err := cp.verifySignature(verifData)
	if err != nil {
		return fmt.Errorf("auth error: %w", err)
	}

	return nil
}

func multipleReqSignatures(m *protosession.RequestMetaHeader) bool {
	return m == nil || m.Version == nil || m.Version.Major < 2 || (m.Version.Major == 2 && m.Version.Minor < 26)
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
