package acl

import (
	"bytes"
	"context"

	acl "github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	core "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/pkg/errors"
)

type (
	// BasicChecker checks basic ACL rules.
	BasicChecker struct {
		containers core.Source
		sender     SenderClassifier
		next       object.Service
	}

	putStreamBasicChecker struct {
		source *BasicChecker
		next   object.PutObjectStreamer
	}

	getStreamBasicChecker struct {
		next object.GetObjectStreamer
		info requestInfo
	}

	searchStreamBasicChecker struct {
		object.SearchObjectStreamer
	}

	getRangeStreamBasicChecker struct {
		object.GetRangeObjectStreamer
	}

	requestInfo struct {
		basicACL    uint32
		requestRole acl.Role
		operation   acl.Operation // put, get, head, etc.
		owner       *owner.ID     // container owner
	}
)

var (
	ErrMalformedRequest  = errors.New("malformed request")
	ErrUnknownRole       = errors.New("can't classify request sender")
	ErrUnknownContainer  = errors.New("can't fetch container info")
	ErrBasicAccessDenied = errors.New("access denied by basic acl")
)

// NewBasicChecker is a constructor for basic ACL checker of object requests.
func NewBasicChecker(
	c SenderClassifier,
	cnr core.Source,
	next object.Service) BasicChecker {

	return BasicChecker{
		containers: cnr,
		sender:     c,
		next:       next,
	}
}

func (b BasicChecker) Get(
	ctx context.Context,
	request *object.GetRequest) (object.GetObjectStreamer, error) {

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	reqInfo, err := b.findRequestInfo(request, cid, acl.OperationGet)
	if err != nil {
		return nil, err
	}

	if !basicACLCheck(reqInfo) {
		return nil, ErrBasicAccessDenied
	}

	stream, err := b.next.Get(ctx, request)

	return getStreamBasicChecker{
		next: stream,
		info: reqInfo,
	}, err
}

func (b BasicChecker) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	streamer, err := b.next.Put(ctx)

	return putStreamBasicChecker{
		source: &b,
		next:   streamer,
	}, err
}

func (b BasicChecker) Head(
	ctx context.Context,
	request *object.HeadRequest) (*object.HeadResponse, error) {

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	reqInfo, err := b.findRequestInfo(request, cid, acl.OperationHead)
	if err != nil {
		return nil, err
	}

	if !basicACLCheck(reqInfo) {
		return nil, ErrBasicAccessDenied
	}

	return b.next.Head(ctx, request)
}

func (b BasicChecker) Search(
	ctx context.Context,
	request *object.SearchRequest) (object.SearchObjectStreamer, error) {

	var cid *container.ID

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	reqInfo, err := b.findRequestInfo(request, cid, acl.OperationSearch)
	if err != nil {
		return nil, err
	}

	if !basicACLCheck(reqInfo) {
		return nil, ErrBasicAccessDenied
	}

	stream, err := b.next.Search(ctx, request)
	return searchStreamBasicChecker{stream}, err
}

func (b BasicChecker) Delete(
	ctx context.Context,
	request *object.DeleteRequest) (*object.DeleteResponse, error) {

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	reqInfo, err := b.findRequestInfo(request, cid, acl.OperationDelete)
	if err != nil {
		return nil, err
	}

	if !basicACLCheck(reqInfo) {
		return nil, ErrBasicAccessDenied
	}

	return b.next.Delete(ctx, request)
}

func (b BasicChecker) GetRange(
	ctx context.Context,
	request *object.GetRangeRequest) (object.GetRangeObjectStreamer, error) {

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	reqInfo, err := b.findRequestInfo(request, cid, acl.OperationRange)
	if err != nil {
		return nil, err
	}

	if !basicACLCheck(reqInfo) {
		return nil, ErrBasicAccessDenied
	}

	stream, err := b.next.GetRange(ctx, request)
	return getRangeStreamBasicChecker{stream}, err
}

func (b BasicChecker) GetRangeHash(
	ctx context.Context,
	request *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {

	cid, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	reqInfo, err := b.findRequestInfo(request, cid, acl.OperationRangeHash)
	if err != nil {
		return nil, err
	}

	if !basicACLCheck(reqInfo) {
		return nil, ErrBasicAccessDenied
	}

	return b.next.GetRangeHash(ctx, request)
}

func (p putStreamBasicChecker) Send(request *object.PutRequest) error {
	body := request.GetBody()
	if body == nil {
		return ErrMalformedRequest
	}

	part := body.GetObjectPart()
	if _, ok := part.(*object.PutObjectPartInit); ok {
		cid, err := getContainerIDFromRequest(request)
		if err != nil {
			return err
		}

		owner, err := getObjectOwnerFromMessage(request)
		if err != nil {
			return err
		}

		reqInfo, err := p.source.findRequestInfo(request, cid, acl.OperationPut)
		if err != nil {
			return err
		}

		if !basicACLCheck(reqInfo) || !stickyBitCheck(reqInfo, owner) {
			return ErrBasicAccessDenied
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
			return nil, ErrBasicAccessDenied
		}
	}

	return resp, err
}

func (b BasicChecker) findRequestInfo(
	req RequestV2,
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

	info.basicACL = cnr.GetBasicACL()
	info.requestRole = role
	info.operation = op
	info.owner = owner.NewIDFromV2(cnr.GetOwnerID())

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
	rule := basicACLHelper(info.basicACL)

	// check basic ACL permissions
	var checkFn func(acl.Operation) bool

	switch info.requestRole {
	case acl.RoleUser:
		checkFn = rule.UserAllowed
	case acl.RoleSystem:
		checkFn = rule.SystemAllowed
	case acl.RoleOthers:
		checkFn = rule.OthersAllowed
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

	rule := basicACLHelper(info.basicACL)
	if !rule.Sticky() {
		return true
	}

	return bytes.Equal(owner.ToV2().GetValue(), info.owner.ToV2().GetValue())
}
