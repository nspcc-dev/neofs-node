package acl

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"

	acl "github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/util/signature"
	bearer "github.com/nspcc-dev/neofs-api-go/v2/acl"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	v2signature "github.com/nspcc-dev/neofs-api-go/v2/signature"
	crypto "github.com/nspcc-dev/neofs-crypto"
	core "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
	eaclV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl/v2"
	"github.com/pkg/errors"
)

type (
	// Service checks basic ACL rules.
	Service struct {
		*cfg
	}

	putStreamBasicChecker struct {
		source *Service
		next   object.PutObjectStreamer

		*eACLCfg
	}

	getStreamBasicChecker struct {
		objectSvc.GetObjectStream

		info requestInfo

		*eACLCfg
	}

	rangeStreamBasicChecker struct {
		objectSvc.GetObjectRangeStream

		info requestInfo

		*eACLCfg
	}

	searchStreamBasicChecker struct {
		objectSvc.SearchStream

		info requestInfo

		*eACLCfg
	}

	requestInfo struct {
		basicACL    basicACLHelper
		requestRole acl.Role
		isInnerRing bool
		operation   acl.Operation // put, get, head, etc.
		cnrOwner    *owner.ID     // container owner

		cid *container.ID

		oid *objectSDK.ID

		senderKey []byte

		bearer *bearer.BearerToken // bearer token of request
	}
)

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	containers core.Source

	sender SenderClassifier

	next objectSvc.ServiceServer

	*eACLCfg
}

type eACLCfg struct {
	eACLOpts []eacl.Option

	eACL *eacl.Validator

	localStorage *engine.StorageEngine

	state netmap.State
}

type accessErr struct {
	requestInfo

	failedCheckTyp string
}

var (
	ErrMalformedRequest = errors.New("malformed request")
	ErrInternal         = errors.New("internal error")
	ErrUnknownRole      = errors.New("can't classify request sender")
	ErrUnknownContainer = errors.New("can't fetch container info")
)

func defaultCfg() *cfg {
	return &cfg{
		eACLCfg: new(eACLCfg),
	}
}

// New is a constructor for object ACL checking service.
func New(opts ...Option) Service {
	cfg := defaultCfg()

	for i := range opts {
		opts[i](cfg)
	}

	cfg.eACL = eacl.NewValidator(cfg.eACLOpts...)

	return Service{
		cfg: cfg,
	}
}

func (b Service) Get(request *object.GetRequest, stream objectSvc.GetObjectStream) error {
	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return err
	}

	sTok := request.GetMetaHeader().GetSessionToken()

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  request.GetMetaHeader().GetBearerToken(),
	}

	reqInfo, err := b.findRequestInfo(req, cid, acl.OperationGet)
	if err != nil {
		return err
	}

	reqInfo.oid = getObjectIDFromRequestBody(request.GetBody())
	useObjectIDFromSession(&reqInfo, sTok)

	if !basicACLCheck(reqInfo) {
		return basicACLErr(reqInfo)
	} else if !eACLCheck(request, reqInfo, b.eACLCfg) {
		return eACLErr(reqInfo)
	}

	return b.next.Get(request, &getStreamBasicChecker{
		GetObjectStream: stream,
		info:            reqInfo,
		eACLCfg:         b.eACLCfg,
	})
}

func (b Service) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	streamer, err := b.next.Put(ctx)

	return putStreamBasicChecker{
		source:  &b,
		next:    streamer,
		eACLCfg: b.eACLCfg,
	}, err
}

func (b Service) Head(
	ctx context.Context,
	request *object.HeadRequest) (*object.HeadResponse, error) {

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	sTok := request.GetMetaHeader().GetSessionToken()

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  request.GetMetaHeader().GetBearerToken(),
	}

	reqInfo, err := b.findRequestInfo(req, cid, acl.OperationHead)
	if err != nil {
		return nil, err
	}

	reqInfo.oid = getObjectIDFromRequestBody(request.GetBody())
	useObjectIDFromSession(&reqInfo, sTok)

	if !basicACLCheck(reqInfo) {
		return nil, basicACLErr(reqInfo)
	} else if !eACLCheck(request, reqInfo, b.eACLCfg) {
		return nil, eACLErr(reqInfo)
	}

	resp, err := b.next.Head(ctx, request)
	if err == nil {
		if !eACLCheck(resp, reqInfo, b.eACLCfg) {
			err = eACLErr(reqInfo)
		}
	}

	return resp, err
}

func (b Service) Search(request *object.SearchRequest, stream objectSvc.SearchStream) error {
	var cid *container.ID

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return err
	}

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   request.GetMetaHeader().GetSessionToken(),
		bearer:  request.GetMetaHeader().GetBearerToken(),
	}

	reqInfo, err := b.findRequestInfo(req, cid, acl.OperationSearch)
	if err != nil {
		return err
	}

	reqInfo.oid = getObjectIDFromRequestBody(request.GetBody())

	if !basicACLCheck(reqInfo) {
		return basicACLErr(reqInfo)
	} else if !eACLCheck(request, reqInfo, b.eACLCfg) {
		return eACLErr(reqInfo)
	}

	return b.next.Search(request, &searchStreamBasicChecker{
		SearchStream: stream,
		info:         reqInfo,
		eACLCfg:      b.eACLCfg,
	})
}

func (b Service) Delete(
	ctx context.Context,
	request *object.DeleteRequest) (*object.DeleteResponse, error) {

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	sTok := request.GetMetaHeader().GetSessionToken()

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  request.GetMetaHeader().GetBearerToken(),
	}

	reqInfo, err := b.findRequestInfo(req, cid, acl.OperationDelete)
	if err != nil {
		return nil, err
	}

	reqInfo.oid = getObjectIDFromRequestBody(request.GetBody())
	useObjectIDFromSession(&reqInfo, sTok)

	if !basicACLCheck(reqInfo) {
		return nil, basicACLErr(reqInfo)
	} else if !eACLCheck(request, reqInfo, b.eACLCfg) {
		return nil, eACLErr(reqInfo)
	}

	return b.next.Delete(ctx, request)
}

func (b Service) GetRange(request *object.GetRangeRequest, stream objectSvc.GetObjectRangeStream) error {
	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return err
	}

	sTok := request.GetMetaHeader().GetSessionToken()

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  request.GetMetaHeader().GetBearerToken(),
	}

	reqInfo, err := b.findRequestInfo(req, cid, acl.OperationRange)
	if err != nil {
		return err
	}

	reqInfo.oid = getObjectIDFromRequestBody(request.GetBody())
	useObjectIDFromSession(&reqInfo, sTok)

	if !basicACLCheck(reqInfo) {
		return basicACLErr(reqInfo)
	} else if !eACLCheck(request, reqInfo, b.eACLCfg) {
		return eACLErr(reqInfo)
	}

	return b.next.GetRange(request, &rangeStreamBasicChecker{
		GetObjectRangeStream: stream,
		info:                 reqInfo,
		eACLCfg:              b.eACLCfg,
	})
}

func (b Service) GetRangeHash(
	ctx context.Context,
	request *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	sTok := request.GetMetaHeader().GetSessionToken()

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  request.GetMetaHeader().GetBearerToken(),
	}

	reqInfo, err := b.findRequestInfo(req, cid, acl.OperationRangeHash)
	if err != nil {
		return nil, err
	}

	reqInfo.oid = getObjectIDFromRequestBody(request.GetBody())
	useObjectIDFromSession(&reqInfo, sTok)

	if !basicACLCheck(reqInfo) {
		return nil, basicACLErr(reqInfo)
	} else if !eACLCheck(request, reqInfo, b.eACLCfg) {
		return nil, eACLErr(reqInfo)
	}

	return b.next.GetRangeHash(ctx, request)
}

func (p putStreamBasicChecker) Send(request *object.PutRequest) error {
	body := request.GetBody()
	if body == nil {
		return ErrMalformedRequest
	}

	part := body.GetObjectPart()
	if part, ok := part.(*object.PutObjectPartInit); ok {
		cid, err := getContainerIDFromRequest(request)
		if err != nil {
			return err
		}

		ownerID, err := getObjectOwnerFromMessage(request)
		if err != nil {
			return err
		}

		sTok := part.GetHeader().GetSessionToken()

		req := metaWithToken{
			vheader: request.GetVerificationHeader(),
			token:   sTok,
			bearer:  request.GetMetaHeader().GetBearerToken(),
		}

		reqInfo, err := p.source.findRequestInfo(req, cid, acl.OperationPut)
		if err != nil {
			return err
		}

		reqInfo.oid = getObjectIDFromRequestBody(part)
		useObjectIDFromSession(&reqInfo, sTok)

		if !basicACLCheck(reqInfo) || !stickyBitCheck(reqInfo, ownerID) {
			return basicACLErr(reqInfo)
		} else if !eACLCheck(request, reqInfo, p.eACLCfg) {
			return eACLErr(reqInfo)
		}
	}

	return p.next.Send(request)
}

func (p putStreamBasicChecker) CloseAndRecv() (*object.PutResponse, error) {
	return p.next.CloseAndRecv()
}

func (g *getStreamBasicChecker) Send(resp *object.GetResponse) error {
	if _, ok := resp.GetBody().GetObjectPart().(*object.GetObjectPartInit); ok {
		if !eACLCheck(resp, g.info, g.eACLCfg) {
			return eACLErr(g.info)
		}
	}

	return g.GetObjectStream.Send(resp)
}

func (g *rangeStreamBasicChecker) Send(resp *object.GetRangeResponse) error {
	if !eACLCheck(resp, g.info, g.eACLCfg) {
		return eACLErr(g.info)
	}

	return g.GetObjectRangeStream.Send(resp)
}

func (g *searchStreamBasicChecker) Send(resp *object.SearchResponse) error {
	if !eACLCheck(resp, g.info, g.eACLCfg) {
		return eACLErr(g.info)
	}

	return g.SearchStream.Send(resp)
}

func (b Service) findRequestInfo(
	req metaWithToken,
	cid *container.ID,
	op acl.Operation) (info requestInfo, err error) {

	// fetch actual container
	cnr, err := b.containers.Get(cid)
	if err != nil || cnr.OwnerID() == nil {
		return info, ErrUnknownContainer
	}

	// find request role and key
	role, isIR, key, err := b.sender.Classify(req, cid, cnr)
	if err != nil {
		return info, err
	}

	if role == acl.RoleUnknown {
		return info, ErrUnknownRole
	}

	// find verb from token if it is present
	verb := sourceVerbOfRequest(req, op)
	// todo: check verb sanity, if it was generated correctly. Do we need it ?

	info.basicACL = basicACLHelper(cnr.BasicACL())
	info.requestRole = role
	info.isInnerRing = isIR
	info.operation = verb
	info.cnrOwner = cnr.OwnerID()
	info.cid = cid

	// it is assumed that at the moment the key will be valid,
	// otherwise the request would not pass validation
	info.senderKey = key

	// add bearer token if it is present in request
	info.bearer = req.bearer

	return info, nil
}

func getContainerIDFromRequest(req interface{}) (id *container.ID, err error) {
	switch v := req.(type) {
	case *object.GetRequest:
		return container.NewIDFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	case *object.PutRequest:
		objPart := v.GetBody().GetObjectPart()
		if part, ok := objPart.(*object.PutObjectPartInit); ok {
			return container.NewIDFromV2(part.GetHeader().GetContainerID()), nil
		} else {
			return nil, errors.New("can't get cid in chunk")
		}
	case *object.HeadRequest:
		return container.NewIDFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	case *object.SearchRequest:
		return container.NewIDFromV2(v.GetBody().GetContainerID()), nil
	case *object.DeleteRequest:
		return container.NewIDFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	case *object.GetRangeRequest:
		return container.NewIDFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	case *object.GetRangeHashRequest:
		return container.NewIDFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	default:
		return nil, errors.New("unknown request type")
	}
}

func useObjectIDFromSession(req *requestInfo, token *session.SessionToken) {
	if token == nil {
		return
	}

	objCtx, ok := token.GetBody().GetContext().(*session.ObjectSessionContext)
	if !ok {
		return
	}

	req.oid = objectSDK.NewIDFromV2(
		objCtx.GetAddress().GetObjectID(),
	)
}

func getObjectIDFromRequestBody(body interface{}) *objectSDK.ID {
	switch v := body.(type) {
	default:
		return nil
	case interface {
		GetObjectID() *refs.ObjectID
	}:
		return objectSDK.NewIDFromV2(v.GetObjectID())
	case interface {
		GetAddress() *refs.Address
	}:
		return objectSDK.NewIDFromV2(v.GetAddress().GetObjectID())
	}
}

func getObjectOwnerFromMessage(req interface{}) (id *owner.ID, err error) {
	switch v := req.(type) {
	case *object.PutRequest:
		objPart := v.GetBody().GetObjectPart()
		if part, ok := objPart.(*object.PutObjectPartInit); ok {
			return owner.NewIDFromV2(part.GetHeader().GetOwnerID()), nil
		} else {
			return nil, errors.New("can't get cid in chunk")
		}
	case *object.GetResponse:
		objPart := v.GetBody().GetObjectPart()
		if part, ok := objPart.(*object.GetObjectPartInit); ok {
			return owner.NewIDFromV2(part.GetHeader().GetOwnerID()), nil
		} else {
			return nil, errors.New("can't get cid in chunk")
		}
	default:
		return nil, errors.New("unsupported request type")
	}

}

// main check function for basic ACL
func basicACLCheck(info requestInfo) bool {
	// check basic ACL permissions
	var checkFn func(acl.Operation) bool

	switch info.requestRole {
	case acl.RoleUser:
		checkFn = info.basicACL.UserAllowed
	case acl.RoleSystem:
		checkFn = info.basicACL.SystemAllowed
		if info.isInnerRing {
			checkFn = info.basicACL.InnerRingAllowed
		}
	case acl.RoleOthers:
		checkFn = info.basicACL.OthersAllowed
	default:
		// log there
		return false
	}

	return checkFn(info.operation)
}

func stickyBitCheck(info requestInfo, owner *owner.ID) bool {
	if owner == nil || len(info.senderKey) == 0 {
		return false
	}

	if !info.basicACL.Sticky() {
		return true
	}

	requestSenderKey := crypto.UnmarshalPublicKey(info.senderKey)

	return isOwnerFromKey(owner, requestSenderKey)
}

func eACLCheck(msg interface{}, reqInfo requestInfo, cfg *eACLCfg) bool {
	if reqInfo.basicACL.Final() {
		return true
	}

	// if bearer token is not allowed, then ignore it
	if !reqInfo.basicACL.BearerAllowed(reqInfo.operation) {
		reqInfo.bearer = nil
	}

	// if bearer token is not present, isValidBearer returns true
	if !isValidBearer(reqInfo, cfg.state) {
		return false
	}

	hdrSrcOpts := make([]eaclV2.Option, 0, 3)

	addr := objectSDK.NewAddress()
	addr.SetContainerID(reqInfo.cid)
	addr.SetObjectID(reqInfo.oid)

	hdrSrcOpts = append(hdrSrcOpts,
		eaclV2.WithLocalObjectStorage(cfg.localStorage),
		eaclV2.WithAddress(addr.ToV2()),
	)

	if req, ok := msg.(eaclV2.Request); ok {
		hdrSrcOpts = append(hdrSrcOpts, eaclV2.WithServiceRequest(req))
	} else {
		hdrSrcOpts = append(hdrSrcOpts, eaclV2.WithServiceResponse(msg.(eaclV2.Response)))
	}

	action := cfg.eACL.CalculateAction(new(eacl.ValidationUnit).
		WithRole(reqInfo.requestRole).
		WithOperation(reqInfo.operation).
		WithContainerID(reqInfo.cid).
		WithSenderKey(reqInfo.senderKey).
		WithHeaderSource(
			eaclV2.NewMessageHeaderSource(hdrSrcOpts...),
		).
		WithBearerToken(reqInfo.bearer),
	)

	return action == acl.ActionAllow
}

// sourceVerbOfRequest looks for verb in session token and if it is not found,
// returns reqVerb.
func sourceVerbOfRequest(req metaWithToken, reqVerb acl.Operation) acl.Operation {
	if req.token != nil {
		switch v := req.token.GetBody().GetContext().(type) {
		case *session.ObjectSessionContext:
			return tokenVerbToOperation(v.GetVerb())
		default:
			// do nothing, return request verb
		}
	}

	return reqVerb
}

func tokenVerbToOperation(verb session.ObjectSessionVerb) acl.Operation {
	switch verb {
	case session.ObjectVerbGet:
		return acl.OperationGet
	case session.ObjectVerbPut:
		return acl.OperationPut
	case session.ObjectVerbHead:
		return acl.OperationHead
	case session.ObjectVerbSearch:
		return acl.OperationSearch
	case session.ObjectVerbDelete:
		return acl.OperationDelete
	case session.ObjectVerbRange:
		return acl.OperationRange
	case session.ObjectVerbRangeHash:
		return acl.OperationRangeHash
	default:
		return acl.OperationUnknown
	}
}

func (a *accessErr) Error() string {
	return fmt.Sprintf("access to operation %v is denied by %s check", a.operation, a.failedCheckTyp)
}

func basicACLErr(info requestInfo) error {
	return &accessErr{
		requestInfo:    info,
		failedCheckTyp: "basic ACL",
	}
}

func eACLErr(info requestInfo) error {
	return &accessErr{
		requestInfo:    info,
		failedCheckTyp: "extended ACL",
	}
}

// isValidBearer returns true if bearer token correctly signed by authorized
// entity. This method might be define on whole ACL service because it will
// require to fetch current epoch to check lifetime.
func isValidBearer(reqInfo requestInfo, st netmap.State) bool {
	token := reqInfo.bearer

	// 0. Check if bearer token is present in reqInfo. It might be non nil
	// empty structure.
	if token == nil || (token.GetBody() == nil && token.GetSignature() == nil) {
		return true
	}

	// 1. First check token lifetime. Simplest verification.
	if !isValidLifetime(token.GetBody().GetLifetime(), st.CurrentEpoch()) {
		return false
	}

	// 2. Then check if bearer token is signed correctly.
	signWrapper := v2signature.StableMarshalerWrapper{SM: token.GetBody()}
	if err := signature.VerifyDataWithSource(signWrapper, func() (key, sig []byte) {
		tokenSignature := token.GetSignature()
		return tokenSignature.GetKey(), tokenSignature.GetSign()
	}); err != nil {
		return false // invalid signature
	}

	// 3. Then check if container owner signed this token.
	tokenIssuerKey := crypto.UnmarshalPublicKey(token.GetSignature().GetKey())
	if !isOwnerFromKey(reqInfo.cnrOwner, tokenIssuerKey) {
		// todo: in this case we can issue all owner keys from neofs.id and check once again
		return false
	}

	// 4. Then check if request sender has rights to use this token.
	tokenOwnerField := owner.NewIDFromV2(token.GetBody().GetOwnerID())
	if tokenOwnerField != nil { // see bearer token owner field description
		requestSenderKey := crypto.UnmarshalPublicKey(reqInfo.senderKey)
		if !isOwnerFromKey(tokenOwnerField, requestSenderKey) {
			// todo: in this case we can issue all owner keys from neofs.id and check once again
			return false
		}
	}

	return true
}

func isValidLifetime(lifetime *bearer.TokenLifetime, epoch uint64) bool {
	// The "exp" (expiration time) claim identifies the expiration time on
	// or after which the JWT MUST NOT be accepted for processing.
	// The "nbf" (not before) claim identifies the time before which the JWT
	// MUST NOT be accepted for processing
	// RFC 7519 sections 4.1.4, 4.1.5
	return epoch >= lifetime.GetNbf() && epoch <= lifetime.GetExp()
}

func isOwnerFromKey(id *owner.ID, key *ecdsa.PublicKey) bool {
	if id == nil || key == nil {
		return false
	}

	wallet, err := owner.NEO3WalletFromPublicKey(key)
	if err != nil {
		return false
	}

	// here we compare `owner.ID -> wallet` with `wallet <- publicKey`
	// consider making equal method on owner.ID structure
	// we can compare .String() version of owners but don't think it is good idea
	// binary comparison is better but MarshalBinary is more expensive
	return bytes.Equal(id.ToV2().GetValue(), wallet.Bytes())
}
