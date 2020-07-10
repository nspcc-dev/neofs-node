package object

import (
	"bytes"
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/acl"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	crypto "github.com/nspcc-dev/neofs-crypto"
	libacl "github.com/nspcc-dev/neofs-node/lib/acl"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/ir"
	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// RequestTargeter is an interface of request's ACL target calculator.
	RequestTargeter interface {
		Target(context.Context, serviceRequest) acl.Target
	}

	// aclPreProcessor is an implementation of requestPreProcessor interface.
	aclPreProcessor struct {
		log *zap.Logger

		aclInfoReceiver aclInfoReceiver

		basicChecker libacl.BasicChecker

		reqActionCalc requestActionCalculator

		localStore localstore.Localstore

		extACLSource libacl.ExtendedACLSource

		bearerVerifier bearerTokenVerifier
	}

	targetFinder struct {
		log *zap.Logger

		irStorage       ir.Storage
		cnrLister       implementations.ContainerNodesLister
		cnrOwnerChecker implementations.ContainerOwnerChecker
	}
)

type objectHeadersSource interface {
	getHeaders() (*Object, bool)
}

type requestActionCalculator interface {
	calculateRequestAction(context.Context, requestActionParams) acl.ExtendedACLAction
}

type aclInfoReceiver struct {
	basicACLGetter implementations.BasicACLGetter

	basicChecker libacl.BasicChecker

	targetFinder RequestTargeter
}

type aclInfo struct {
	rule uint32

	checkExtended bool

	checkBearer bool

	target acl.Target
}

type reqActionCalc struct {
	extACLChecker libacl.ExtendedACLChecker

	log *zap.Logger
}

type serviceRequestInfo struct {
	target acl.Target

	req serviceRequest

	objHdrSrc objectHeadersSource
}

type requestObjHdrSrc struct {
	req serviceRequest

	ls localstore.Localstore
}

type eaclFromBearer struct {
	bearer service.BearerToken
}

var _ requestPreProcessor = (*aclPreProcessor)(nil)

var errMissingSignatures = errors.New("empty signature list")

func (p *aclPreProcessor) preProcess(ctx context.Context, req serviceRequest) error {
	if req == nil {
		panic(pmEmptyServiceRequest)
	}

	// fetch ACL info
	aclInfo, err := p.aclInfoReceiver.getACLInfo(ctx, req)
	if err != nil {
		p.log.Warn("can't get acl of the container", zap.Stringer("cid", req.CID()))
		return errAccessDenied
	}

	// check basic ACL permissions
	allow, err := p.basicChecker.Action(aclInfo.rule, req.Type(), aclInfo.target)
	if err != nil || !allow {
		return errAccessDenied
	}

	if aclInfo.target != acl.Target_System &&
		p.basicChecker.Sticky(aclInfo.rule) &&
		!checkObjectRequestOwnerMatch(req) {
		return errAccessDenied
	}

	if !aclInfo.checkBearer && !aclInfo.checkExtended {
		return nil
	}

	actionParams := requestActionParams{
		eaclSrc: p.extACLSource,
		request: req,
		objHdrSrc: &requestObjHdrSrc{
			req: req,
			ls:  p.localStore,
		},
		target: aclInfo.target,
	}

	if aclInfo.checkBearer {
		bearer := req.GetBearerToken()

		if err := p.bearerVerifier.verifyBearerToken(ctx, req.CID(), bearer); err != nil {
			p.log.Warn("bearer token verification failure",
				zap.String("error", err.Error()),
			)

			return errAccessDenied
		}

		actionParams.eaclSrc = eaclFromBearer{
			bearer: bearer,
		}
	}

	if p.reqActionCalc.calculateRequestAction(ctx, actionParams) != acl.ActionAllow {
		return errAccessDenied
	}

	return nil
}

func (t *targetFinder) Target(ctx context.Context, req serviceRequest) acl.Target {
	ownerID, ownerKey, err := requestOwner(req)
	if err != nil {
		t.log.Warn("could not get request owner",
			zap.String("error", err.Error()),
		)

		return acl.Target_Unknown
	} else if ownerKey == nil {
		t.log.Warn("signature with nil public key detected")
		return acl.Target_Unknown
	}

	// if request from container owner then return Target_User
	isOwner, err := t.cnrOwnerChecker.IsContainerOwner(ctx, req.CID(), ownerID)
	if err != nil {
		t.log.Warn("can't check container owner", zap.String("err", err.Error()))
		return acl.Target_Unknown
	} else if isOwner {
		return acl.Target_User
	}

	ownerKeyBytes := crypto.MarshalPublicKey(ownerKey)

	// if request from inner ring then return Target_System
	isIRKey, err := ir.IsInnerRingKey(t.irStorage, ownerKeyBytes)
	if err != nil {
		t.log.Warn("could not verify the key belongs to the node", zap.String("err", err.Error()))
		return acl.Target_Unknown
	} else if isIRKey {
		return acl.Target_System
	}

	// if request from current container node then return Target_System
	cnr, err := t.cnrLister.ContainerNodesInfo(ctx, req.CID(), 0)
	if err != nil {
		t.log.Warn("can't get current container list", zap.String("err", err.Error()))
		return acl.Target_Unknown
	}

	for i := range cnr {
		if bytes.Equal(cnr[i].PubKey, ownerKeyBytes) {
			return acl.Target_System
		}
	}

	// if request from previous container node then return Target_System
	cnr, err = t.cnrLister.ContainerNodesInfo(ctx, req.CID(), 1)
	if err != nil {
		t.log.Warn("can't get previous container list", zap.String("err", err.Error()))
		return acl.Target_Unknown
	}

	for i := range cnr {
		if bytes.Equal(cnr[i].PubKey, ownerKeyBytes) {
			return acl.Target_System
		}
	}

	// if none of the above return Target_Others
	return acl.Target_Others
}

func checkObjectRequestOwnerMatch(req serviceRequest) bool {
	rt := req.Type()

	// ignore all request types except Put and Delete
	if rt != object.RequestPut && rt != object.RequestDelete {
		return true
	}

	// get request owner
	reqOwner, _, err := requestOwner(req)
	if err != nil {
		return false
	}

	var payloadOwner OwnerID

	// get owner from request payload
	if rt == object.RequestPut {
		obj := req.(transport.PutInfo).GetHead()
		if obj == nil {
			return false
		}

		payloadOwner = obj.GetSystemHeader().OwnerID
	} else {
		payloadOwner = req.(*object.DeleteRequest).OwnerID
	}

	return reqOwner.Equal(payloadOwner)
}

// FIXME: this solution only works with healthy key-to-owner conversion.
func requestOwner(req serviceRequest) (OwnerID, *ecdsa.PublicKey, error) {
	// if session token exists => return its owner
	if token := req.GetSessionToken(); token != nil {
		return token.GetOwnerID(), crypto.UnmarshalPublicKey(token.GetOwnerKey()), nil
	}

	signKeys := req.GetSignKeyPairs()
	if len(signKeys) == 0 {
		return OwnerID{}, nil, errMissingSignatures
	}

	firstKey := signKeys[0].GetPublicKey()
	if firstKey == nil {
		return OwnerID{}, nil, crypto.ErrEmptyPublicKey
	}

	owner, err := refs.NewOwnerID(firstKey)

	return owner, firstKey, err
}

// HeadersOfType returns request or object headers.
func (s serviceRequestInfo) HeadersOfType(typ acl.HeaderType) ([]acl.Header, bool) {
	switch typ {
	default:
		return nil, true
	case acl.HdrTypeRequest:
		return libacl.TypedHeaderSourceFromExtendedHeaders(s.req).HeadersOfType(typ)
	case acl.HdrTypeObjSys, acl.HdrTypeObjUsr:
		obj, ok := s.objHdrSrc.getHeaders()
		if !ok {
			return nil, false
		}

		return libacl.TypedHeaderSourceFromObject(obj).HeadersOfType(typ)
	}
}

// Key returns a binary representation of sender public key.
func (s serviceRequestInfo) Key() []byte {
	_, key, err := requestOwner(s.req)
	if err != nil {
		return nil
	}

	return crypto.MarshalPublicKey(key)
}

// TypeOf returns true of object request type corresponds to passed OperationType.
func (s serviceRequestInfo) TypeOf(opType acl.OperationType) bool {
	switch s.req.Type() {
	case object.RequestGet:
		return opType == acl.OpTypeGet
	case object.RequestPut:
		return opType == acl.OpTypePut
	case object.RequestHead:
		return opType == acl.OpTypeHead
	case object.RequestSearch:
		return opType == acl.OpTypeSearch
	case object.RequestDelete:
		return opType == acl.OpTypeDelete
	case object.RequestRange:
		return opType == acl.OpTypeRange
	case object.RequestRangeHash:
		return opType == acl.OpTypeRangeHash
	default:
		return false
	}
}

// TargetOf return true if target field is equal to passed ACL target.
func (s serviceRequestInfo) TargetOf(target acl.Target) bool {
	return s.target == target
}

func (s requestObjHdrSrc) getHeaders() (*Object, bool) {
	switch s.req.Type() {
	case object.RequestSearch:
		// object header filters is not supported in Search request now
		return nil, true
	case object.RequestPut:
		// for Put we get object headers from request
		return s.req.(transport.PutInfo).GetHead(), true
	default:
		tReq := &transportRequest{
			serviceRequest: s.req,
		}

		// for other requests we get object headers from local storage
		m, err := s.ls.Meta(tReq.GetAddress())
		if err == nil {
			return m.GetObject(), true
		}

		return nil, false
	}
}

type requestActionParams struct {
	eaclSrc libacl.ExtendedACLSource

	request serviceRequest

	objHdrSrc objectHeadersSource

	target acl.Target
}

func (s reqActionCalc) calculateRequestAction(ctx context.Context, p requestActionParams) acl.ExtendedACLAction {
	// get EACL table
	table, err := p.eaclSrc.GetExtendedACLTable(ctx, p.request.CID())
	if err != nil {
		s.log.Warn("could not get extended acl of the container",
			zap.Stringer("cid", p.request.CID()),
			zap.String("error", err.Error()),
		)

		return acl.ActionUndefined
	}

	// create RequestInfo instance
	reqInfo := &serviceRequestInfo{
		target:    p.target,
		req:       p.request,
		objHdrSrc: p.objHdrSrc,
	}

	// calculate ACL action
	return s.extACLChecker.Action(table, reqInfo)
}

func (s aclInfoReceiver) getACLInfo(ctx context.Context, req serviceRequest) (*aclInfo, error) {
	rule, err := s.basicACLGetter.GetBasicACL(ctx, req.CID())
	if err != nil {
		return nil, err
	}

	isBearer, err := s.basicChecker.Bearer(rule, req.Type())
	if err != nil {
		return nil, err
	}

	// fetch target from the request
	target := s.targetFinder.Target(ctx, req)

	return &aclInfo{
		rule: rule,

		checkExtended: target != acl.Target_System && s.basicChecker.Extended(rule),

		target: target,

		checkBearer: target != acl.Target_System && isBearer && req.GetBearerToken() != nil,
	}, nil
}

func (s eaclFromBearer) GetExtendedACLTable(ctx context.Context, cid CID) (acl.ExtendedACLTable, error) {
	table := acl.WrapEACLTable(nil)

	if err := table.UnmarshalBinary(s.bearer.GetACLRules()); err != nil {
		return nil, err
	}

	return table, nil
}
