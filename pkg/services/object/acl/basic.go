package acl

import (
	"bytes"
	"context"
	"fmt"

	acl "github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	core "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
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
		next object.GetObjectStreamer
		info requestInfo

		*eACLCfg
	}

	searchStreamBasicChecker struct {
		object.SearchObjectStreamer
	}

	getRangeStreamBasicChecker struct {
		object.GetRangeObjectStreamer
	}

	requestInfo struct {
		basicACL    basicACLHelper
		requestRole acl.Role
		operation   acl.Operation // put, get, head, etc.
		owner       *owner.ID     // container owner

		cid *container.ID

		senderKey []byte
	}
)

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	containers core.Source

	sender SenderClassifier

	next object.Service

	*eACLCfg
}

type eACLCfg struct {
	eACLOpts []eacl.Option

	eACL *eacl.Validator

	localStorage *localstore.Storage
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

	cfg.eACL = eacl.NewValidator(cfg.eACLOpts...)

	return Service{
		cfg: cfg,
	}
}

func (b Service) Get(
	ctx context.Context,
	request *object.GetRequest) (object.GetObjectStreamer, error) {
	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   request.GetMetaHeader().GetSessionToken(),
	}

	reqInfo, err := b.findRequestInfo(req, cid, acl.OperationGet)
	if err != nil {
		return nil, err
	}

	if !basicACLCheck(reqInfo) {
		return nil, basicACLErr(reqInfo)
	} else if !eACLCheck(request, reqInfo, b.eACLCfg) {
		return nil, eACLErr(reqInfo)
	}

	stream, err := b.next.Get(ctx, request)

	return getStreamBasicChecker{
		next:    stream,
		info:    reqInfo,
		eACLCfg: b.eACLCfg,
	}, err
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

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   request.GetMetaHeader().GetSessionToken(),
	}

	reqInfo, err := b.findRequestInfo(req, cid, acl.OperationHead)
	if err != nil {
		return nil, err
	}

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

func (b Service) Search(
	ctx context.Context,
	request *object.SearchRequest) (object.SearchObjectStreamer, error) {

	var cid *container.ID

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   request.GetMetaHeader().GetSessionToken(),
	}

	reqInfo, err := b.findRequestInfo(req, cid, acl.OperationSearch)
	if err != nil {
		return nil, err
	}

	if !basicACLCheck(reqInfo) {
		return nil, basicACLErr(reqInfo)
	} else if !eACLCheck(request, reqInfo, b.eACLCfg) {
		return nil, eACLErr(reqInfo)
	}

	stream, err := b.next.Search(ctx, request)
	return searchStreamBasicChecker{stream}, err
}

func (b Service) Delete(
	ctx context.Context,
	request *object.DeleteRequest) (*object.DeleteResponse, error) {

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   request.GetMetaHeader().GetSessionToken(),
	}

	reqInfo, err := b.findRequestInfo(req, cid, acl.OperationDelete)
	if err != nil {
		return nil, err
	}

	if !basicACLCheck(reqInfo) {
		return nil, basicACLErr(reqInfo)
	} else if !eACLCheck(request, reqInfo, b.eACLCfg) {
		return nil, eACLErr(reqInfo)
	}

	return b.next.Delete(ctx, request)
}

func (b Service) GetRange(
	ctx context.Context,
	request *object.GetRangeRequest) (object.GetRangeObjectStreamer, error) {

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   request.GetMetaHeader().GetSessionToken(),
	}

	reqInfo, err := b.findRequestInfo(req, cid, acl.OperationRange)
	if err != nil {
		return nil, err
	}

	if !basicACLCheck(reqInfo) {
		return nil, basicACLErr(reqInfo)
	} else if !eACLCheck(request, reqInfo, b.eACLCfg) {
		return nil, eACLErr(reqInfo)
	}

	stream, err := b.next.GetRange(ctx, request)
	return getRangeStreamBasicChecker{stream}, err
}

func (b Service) GetRangeHash(
	ctx context.Context,
	request *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	req := metaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   request.GetMetaHeader().GetSessionToken(),
	}

	reqInfo, err := b.findRequestInfo(req, cid, acl.OperationRangeHash)
	if err != nil {
		return nil, err
	}

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

		req := metaWithToken{
			vheader: request.GetVerificationHeader(),
			token:   part.GetHeader().GetSessionToken(),
		}

		reqInfo, err := p.source.findRequestInfo(req, cid, acl.OperationPut)
		if err != nil {
			return err
		}

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

func (g getStreamBasicChecker) Recv() (*object.GetResponse, error) {
	resp, err := g.next.Recv()
	if err != nil {
		return resp, err
	}

	body := resp.GetBody()
	if body == nil {
		return resp, err
	}

	part := body.GetObjectPart()
	if _, ok := part.(*object.GetObjectPartInit); ok {
		ownerID, err := getObjectOwnerFromMessage(resp)
		if err != nil {
			return nil, err
		}

		if !stickyBitCheck(g.info, ownerID) {
			return nil, basicACLErr(g.info)
		} else if !eACLCheck(resp, g.info, g.eACLCfg) {
			return nil, eACLErr(g.info)
		}
	}

	return resp, err
}

func (b Service) findRequestInfo(
	req metaWithToken,
	cid *container.ID,
	op acl.Operation) (info requestInfo, err error) {

	// fetch actual container
	cnr, err := b.containers.Get(cid)
	if err != nil || cnr.GetOwnerID() == nil {
		return info, ErrUnknownContainer
	}

	// find request role
	role := b.sender.Classify(req, cid, cnr)
	if role == acl.RoleUnknown {
		return info, ErrUnknownRole
	}

	// find verb from token if it is present
	verb := sourceVerbOfRequest(req, op)
	// todo: check verb sanity, if it was generated correctly. Do we need it ?

	info.basicACL = basicACLHelper(cnr.GetBasicACL())
	info.requestRole = role
	info.operation = verb
	info.owner = owner.NewIDFromV2(cnr.GetOwnerID())

	info.cid = cid

	// it is assumed that at the moment the key will be valid,
	// otherwise the request would not pass validation
	info.senderKey = req.vheader.GetBodySignature().GetKey()

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
	case acl.RoleOthers:
		checkFn = info.basicACL.OthersAllowed
	default:
		// log there
		return false
	}

	return checkFn(info.operation)
}

func stickyBitCheck(info requestInfo, owner *owner.ID) bool {
	if owner == nil || info.owner == nil {
		return false
	}

	if !info.basicACL.Sticky() {
		return true
	}

	return bytes.Equal(owner.ToV2().GetValue(), info.owner.ToV2().GetValue())
}

func eACLCheck(msg interface{}, reqInfo requestInfo, cfg *eACLCfg) bool {
	if reqInfo.basicACL.Final() {
		return true
	}

	hdrSrcOpts := make([]eaclV2.Option, 0, 2)

	hdrSrcOpts = append(hdrSrcOpts, eaclV2.WithLocalObjectStorage(cfg.localStorage))

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
		),
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
