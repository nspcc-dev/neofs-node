package acl

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/pkg/errors"
)

type (
	// ContainerGetter accesses NeoFS container storage.
	// fixme: use core.container interface implementation
	ContainerGetter interface {
		Get(*refs.ContainerID) (*container.Container, error)
	}

	Classifier interface {
		Classify(RequestV2, *refs.ContainerID) acl.Role
	}

	// BasicChecker checks basic ACL rules.
	BasicChecker struct {
		containers ContainerGetter
		sender     SenderClassifier
		next       object.Service
	}

	putStreamBasicChecker struct {
		source *BasicChecker
		next   object.PutObjectStreamer
	}

	getStreamBasicChecker struct {
		object.GetObjectStreamer
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
	}
)

var (
	ErrMalformedRequest  = errors.New("malformed request")
	ErrUnknownRole       = errors.New("can't classify request sender")
	ErrUnknownContainer  = errors.New("can't fetch container info")
	ErrBasicAccessDenied = errors.New("access denied by basic ACL")
)

// NewBasicChecker is a constructor for basic ACL checker of object requests.
func NewBasicChecker(
	c SenderClassifier,
	cnr ContainerGetter,
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
	return getStreamBasicChecker{stream}, err
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

	var cid *refs.ContainerID

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

		reqInfo, err := p.source.findRequestInfo(request, cid, acl.OperationPut)
		if err != nil {
			return err
		}

		if !basicACLCheck(reqInfo) {
			return ErrBasicAccessDenied
		}
	}

	return p.next.Send(request)
}

func (p putStreamBasicChecker) CloseAndRecv() (*object.PutResponse, error) {
	return p.next.CloseAndRecv()
}

func (b BasicChecker) findRequestInfo(
	req RequestV2,
	cid *refs.ContainerID,
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

	return info, nil
}

func getContainerIDFromRequest(req interface{}) (id *refs.ContainerID, err error) {
	defer func() {
		// if there is a NPE on get body and get address
		if r := recover(); r != nil {
			err = ErrMalformedRequest
		}
	}()

	switch v := req.(type) {
	case *object.GetRequest:
		return v.GetBody().GetAddress().GetContainerID(), nil
	case *object.PutRequest:
		objPart := v.GetBody().GetObjectPart()
		if part, ok := objPart.(*object.PutObjectPartInit); ok {
			return part.GetHeader().GetContainerID(), nil
		} else {
			return nil, errors.New("can't get cid in chunk")
		}
	case *object.HeadRequest:
		return v.GetBody().GetAddress().GetContainerID(), nil
	case *object.SearchRequest:
		return v.GetBody().GetContainerID(), nil
	case *object.DeleteRequest:
		return v.GetBody().GetAddress().GetContainerID(), nil
	case *object.GetRangeRequest:
		return v.GetBody().GetAddress().GetContainerID(), nil
	case *object.GetRangeHashRequest:
		return v.GetBody().GetAddress().GetContainerID(), nil
	default:
		return nil, errors.New("unknown request type")
	}
}

func basicACLCheck(info requestInfo) bool {
	panic("implement me")
}
