package acl

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	bearer "github.com/nspcc-dev/neofs-api-go/v2/acl"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	v2signature "github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	core "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
	eaclV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl/v2"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	objectSDKAddress "github.com/nspcc-dev/neofs-sdk-go/object/address"
	objectSDKID "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/util/signature"
)

type (
	// Service checks basic ACL rules.
	Service struct {
		*cfg
	}

	putStreamBasicChecker struct {
		source *Service
		next   objectSvc.PutObjectStream

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
		requestRole eaclSDK.Role
		isInnerRing bool
		operation   eaclSDK.Operation // put, get, head, etc.
		cnrOwner    *owner.ID         // container owner

		cid *cid.ID

		oid *objectSDKID.ID

		senderKey []byte

		bearer *bearer.BearerToken // bearer token of request

		srcRequest interface{}
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
	eaclSource eacl.Source

	eACL *eaclSDK.Validator

	localStorage *engine.StorageEngine

	state netmap.State
}

type accessErr struct {
	requestInfo

	failedCheckTyp string
}

var (
	ErrMalformedRequest = errors.New("malformed request")
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

	cfg.eACL = eaclSDK.NewValidator()

	return Service{
		cfg: cfg,
	}
}

func (b Service) Get(request *object.GetRequest, stream objectSvc.GetObjectStream) error {
	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return err
	}

	sTok := originalSessionToken(request.GetMetaHeader())

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  originalBearerToken(request.GetMetaHeader()),
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cid, eaclSDK.OperationGet)
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

func (b Service) Put(ctx context.Context) (objectSvc.PutObjectStream, error) {
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

	sTok := originalSessionToken(request.GetMetaHeader())

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  originalBearerToken(request.GetMetaHeader()),
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cid, eaclSDK.OperationHead)
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
	var id *cid.ID

	id, err := getContainerIDFromRequest(request)
	if err != nil {
		return err
	}

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   originalSessionToken(request.GetMetaHeader()),
		bearer:  originalBearerToken(request.GetMetaHeader()),
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, id, eaclSDK.OperationSearch)
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

	sTok := originalSessionToken(request.GetMetaHeader())

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  originalBearerToken(request.GetMetaHeader()),
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cid, eaclSDK.OperationDelete)
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

	sTok := originalSessionToken(request.GetMetaHeader())

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  originalBearerToken(request.GetMetaHeader()),
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cid, eaclSDK.OperationRange)
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

	sTok := originalSessionToken(request.GetMetaHeader())

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  originalBearerToken(request.GetMetaHeader()),
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cid, eaclSDK.OperationRangeHash)
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

		sTok := request.GetMetaHeader().GetSessionToken()

		req := metaWithToken{
			vheader: request.GetVerificationHeader(),
			token:   sTok,
			bearer:  originalBearerToken(request.GetMetaHeader()),
			src:     request,
		}

		reqInfo, err := p.source.findRequestInfo(req, cid, eaclSDK.OperationPut)
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
	cid *cid.ID,
	op eaclSDK.Operation) (info requestInfo, err error) {
	cnr, err := b.containers.Get(cid) // fetch actual container
	if err != nil || cnr.OwnerID() == nil {
		return info, ErrUnknownContainer
	}

	// find request role and key
	role, isIR, key, err := b.sender.Classify(req, cid, cnr)
	if err != nil {
		return info, err
	}

	if role == eaclSDK.RoleUnknown {
		return info, ErrUnknownRole
	}

	// find verb from token if it is present
	verb := sourceVerbOfRequest(req, op)

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

	info.srcRequest = req.src

	return info, nil
}

func getContainerIDFromRequest(req interface{}) (id *cid.ID, err error) {
	switch v := req.(type) {
	case *object.GetRequest:
		return cid.NewFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	case *object.PutRequest:
		objPart := v.GetBody().GetObjectPart()
		if part, ok := objPart.(*object.PutObjectPartInit); ok {
			return cid.NewFromV2(part.GetHeader().GetContainerID()), nil
		}

		return nil, errors.New("can't get cid in chunk")
	case *object.HeadRequest:
		return cid.NewFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	case *object.SearchRequest:
		return cid.NewFromV2(v.GetBody().GetContainerID()), nil
	case *object.DeleteRequest:
		return cid.NewFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	case *object.GetRangeRequest:
		return cid.NewFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	case *object.GetRangeHashRequest:
		return cid.NewFromV2(v.GetBody().GetAddress().GetContainerID()), nil
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

	req.oid = objectSDKID.NewIDFromV2(
		objCtx.GetAddress().GetObjectID(),
	)
}

func getObjectIDFromRequestBody(body interface{}) *objectSDKID.ID {
	switch v := body.(type) {
	default:
		return nil
	case interface {
		GetObjectID() *refs.ObjectID
	}:
		return objectSDKID.NewIDFromV2(v.GetObjectID())
	case interface {
		GetAddress() *refs.Address
	}:
		return objectSDKID.NewIDFromV2(v.GetAddress().GetObjectID())
	}
}

func getObjectOwnerFromMessage(req interface{}) (id *owner.ID, err error) {
	switch v := req.(type) {
	case *object.PutRequest:
		objPart := v.GetBody().GetObjectPart()
		if part, ok := objPart.(*object.PutObjectPartInit); ok {
			return owner.NewIDFromV2(part.GetHeader().GetOwnerID()), nil
		}

		return nil, errors.New("can't get cid in chunk")
	case *object.GetResponse:
		objPart := v.GetBody().GetObjectPart()
		if part, ok := objPart.(*object.GetObjectPartInit); ok {
			return owner.NewIDFromV2(part.GetHeader().GetOwnerID()), nil
		}

		return nil, errors.New("can't get cid in chunk")
	default:
		return nil, errors.New("unsupported request type")
	}
}

// main check function for basic ACL
func basicACLCheck(info requestInfo) bool {
	// check basic ACL permissions
	var checkFn func(eaclSDK.Operation) bool

	switch info.requestRole {
	case eaclSDK.RoleUser:
		checkFn = info.basicACL.UserAllowed
	case eaclSDK.RoleSystem:
		checkFn = info.basicACL.SystemAllowed
		if info.isInnerRing {
			checkFn = info.basicACL.InnerRingAllowed
		}
	case eaclSDK.RoleOthers:
		checkFn = info.basicACL.OthersAllowed
	default:
		// log there
		return false
	}

	return checkFn(info.operation)
}

func stickyBitCheck(info requestInfo, owner *owner.ID) bool {
	// According to NeoFS specification sticky bit has no effect on system nodes
	// for correct intra-container work with objects (in particular, replication).
	if info.requestRole == eaclSDK.RoleSystem {
		return true
	}

	if !info.basicACL.Sticky() {
		return true
	}

	if owner == nil || len(info.senderKey) == 0 {
		return false
	}

	requestSenderKey := unmarshalPublicKey(info.senderKey)

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

	var (
		table *eaclSDK.Table
		err   error
	)

	if reqInfo.bearer == nil {
		table, err = cfg.eaclSource.GetEACL(reqInfo.cid)
		if err != nil {
			return errors.Is(err, container.ErrEACLNotFound)
		}
	} else {
		table = eaclSDK.NewTableFromV2(reqInfo.bearer.GetBody().GetEACL())
	}

	// if bearer token is not present, isValidBearer returns true
	if !isValidBearer(reqInfo, cfg.state) {
		return false
	}

	hdrSrcOpts := make([]eaclV2.Option, 0, 3)

	addr := objectSDKAddress.NewAddress()
	addr.SetContainerID(reqInfo.cid)
	addr.SetObjectID(reqInfo.oid)

	hdrSrcOpts = append(hdrSrcOpts,
		eaclV2.WithLocalObjectStorage(cfg.localStorage),
		eaclV2.WithAddress(addr.ToV2()),
	)

	if req, ok := msg.(eaclV2.Request); ok {
		hdrSrcOpts = append(hdrSrcOpts, eaclV2.WithServiceRequest(req))
	} else {
		hdrSrcOpts = append(hdrSrcOpts,
			eaclV2.WithServiceResponse(
				msg.(eaclV2.Response),
				reqInfo.srcRequest.(eaclV2.Request),
			),
		)
	}

	action := cfg.eACL.CalculateAction(new(eaclSDK.ValidationUnit).
		WithRole(reqInfo.requestRole).
		WithOperation(reqInfo.operation).
		WithContainerID(reqInfo.cid).
		WithSenderKey(reqInfo.senderKey).
		WithHeaderSource(
			eaclV2.NewMessageHeaderSource(hdrSrcOpts...),
		).
		WithEACLTable(table),
	)

	return action == eaclSDK.ActionAllow
}

// sourceVerbOfRequest looks for verb in session token and if it is not found,
// returns reqVerb.
func sourceVerbOfRequest(req metaWithToken, reqVerb eaclSDK.Operation) eaclSDK.Operation {
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

func tokenVerbToOperation(verb session.ObjectSessionVerb) eaclSDK.Operation {
	switch verb {
	case session.ObjectVerbGet:
		return eaclSDK.OperationGet
	case session.ObjectVerbPut:
		return eaclSDK.OperationPut
	case session.ObjectVerbHead:
		return eaclSDK.OperationHead
	case session.ObjectVerbSearch:
		return eaclSDK.OperationSearch
	case session.ObjectVerbDelete:
		return eaclSDK.OperationDelete
	case session.ObjectVerbRange:
		return eaclSDK.OperationRange
	case session.ObjectVerbRangeHash:
		return eaclSDK.OperationRangeHash
	default:
		return eaclSDK.OperationUnknown
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
	tokenIssuerKey := unmarshalPublicKey(token.GetSignature().GetKey())
	if !isOwnerFromKey(reqInfo.cnrOwner, tokenIssuerKey) {
		// TODO: #1156 in this case we can issue all owner keys from neofs.id and check once again
		return false
	}

	// 4. Then check if request sender has rights to use this token.
	tokenOwnerField := owner.NewIDFromV2(token.GetBody().GetOwnerID())
	if tokenOwnerField != nil { // see bearer token owner field description
		requestSenderKey := unmarshalPublicKey(reqInfo.senderKey)
		if !isOwnerFromKey(tokenOwnerField, requestSenderKey) {
			// TODO: #1156 in this case we can issue all owner keys from neofs.id and check once again
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

func isOwnerFromKey(id *owner.ID, key *keys.PublicKey) bool {
	if id == nil || key == nil {
		return false
	}

	return id.Equal(owner.NewIDFromPublicKey((*ecdsa.PublicKey)(key)))
}

// originalBearerToken goes down to original request meta header and fetches
// bearer token from there.
func originalBearerToken(header *session.RequestMetaHeader) *bearer.BearerToken {
	for header.GetOrigin() != nil {
		header = header.GetOrigin()
	}

	return header.GetBearerToken()
}

// originalSessionToken goes down to original request meta header and fetches
// session token from there.
func originalSessionToken(header *session.RequestMetaHeader) *session.SessionToken {
	for header.GetOrigin() != nil {
		header = header.GetOrigin()
	}

	return header.GetSessionToken()
}
