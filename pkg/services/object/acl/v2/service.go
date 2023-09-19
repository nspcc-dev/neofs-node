package v2acl

import (
	"context"
	"errors"
	"fmt"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object"
	aclservice "github.com/nspcc-dev/neofs-node/pkg/services/object/acl"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
)

// Service is an intermediate handler of object operations that performs
// access checks.
type Service struct {
	checker *aclservice.Checker

	nextHandler object.ServiceServer
}

// New constructs Service working on top of the provided [aclservice.Node] and
// managing access for all incoming requests. If there is access, any request is
// passed on to the specified next handler, and if there is no access,
// processing is terminated.
func New(node aclservice.Node, nextHandler object.ServiceServer) *Service {
	return &Service{
		checker:     aclservice.NewChecker(node),
		nextHandler: nextHandler,
	}
}

// getStreamWithAccessCheck provides object read stream that performs additional
// access control: it handles object and response headers from incoming response
// messages that are missing in the original request.
type getStreamWithAccessCheck struct {
	// target stream
	object.GetObjectStream

	checker *aclservice.Checker

	// original request's data
	request       request
	cnr           cid.ID
	objID         oid.ID
	bClientPubKey []byte
	sessionToken  *sessionSDK.Object
	bearerToken   *bearer.Token

	// dynamic context

	processedInitialPart bool
}

func newGetStreamWithAccessCheck(
	targetStream object.GetObjectStream,
	checker *aclservice.Checker,
	request request,
	cnr cid.ID,
	objID oid.ID,
	bClientPubKey []byte,
	sessionToken *sessionSDK.Object,
	bearerToken *bearer.Token,
) *getStreamWithAccessCheck {
	return &getStreamWithAccessCheck{
		GetObjectStream: targetStream,
		checker:         checker,
		request:         request,
		cnr:             cnr,
		objID:           objID,
		bClientPubKey:   bClientPubKey,
		sessionToken:    sessionToken,
		bearerToken:     bearerToken,
	}
}

// Send intercepts and verifies a message carrying object headers by passing
// them to the underlying [aclservice.Checker]. Other messages and those that
// pass the check are forwarded to the underlying target stream.
func (x *getStreamWithAccessCheck) Send(resp *objectV2.GetResponse) error {
	body := resp.GetBody()
	if body == nil {
		panic("missing response body")
	}

	var hdrMsg *objectV2.Header

	switch part := body.GetObjectPart().(type) {
	default:
		panic(fmt.Sprintf("unexpected part of the response stream %T", body.GetObjectPart()))
	case *objectV2.GetObjectPartChunk, *objectV2.SplitInfo:
		// just send any message without object headers untouched
		return x.GetObjectStream.Send(resp)
	case *objectV2.GetObjectPartInit:
		if x.processedInitialPart {
			panic(errMultipleStreamInitializers)
		}

		x.processedInitialPart = true

		hdrMsg = part.GetHeader()
		if hdrMsg == nil {
			panic("missing object header in the response message")
		}
	}

	// embed header into object message because objectSDK package doesn't work
	// with header type
	var objMsg objectV2.Object
	objMsg.SetHeader(hdrMsg)

	hdr := objectSDK.NewFromV2(&objMsg)

	r := aclservice.NewContainerRequest(x.cnr, x.bClientPubKey, aclservice.GetResponse(x.objID, *hdr), wrapRequest(x.request))

	if x.bearerToken != nil {
		r.SetBearerToken(*x.bearerToken)
	}

	if x.sessionToken != nil {
		r.SetSessionToken(*x.sessionToken)
	}

	err := x.checker.CheckAccess(r)
	if err != nil {
		var errAccessDenied aclservice.AccessDeniedError

		switch {
		default:
			// here err may be protocol status and returned directly
			return err
		case errors.As(err, &errAccessDenied):
			var e apistatus.ObjectAccessDenied
			e.WriteReason(errAccessDenied.Error())

			return e
		case errors.Is(err, aclservice.ErrNotEnoughData):
			panic(fmt.Sprintf("unexpected error from ACL checker: %v", err))
		}
	}

	return x.GetObjectStream.Send(resp)
}

// Get performs access control for the given request. If access is granted,
// processing is transferred to the underlying handler. Access checks are also
// performed for response messages that carry additional data. Returns:
//   - [apistatus.ObjectAccessDenied] on access denial
//   - [apistatus.ErrSessionTokenExpired] if session token is specified but expired
//   - [apistatus.ErrContainerNotFound] if requested container is missing in the network
func (b *Service) Get(req *objectV2.GetRequest, stream object.GetObjectStream) error {
	bClientPubKey, err := binaryClientPublicKeyFromRequest(req)
	if err != nil {
		return err
	}

	reqBody := req.GetBody()
	if reqBody == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(errMissingRequestBody))
	}

	addr := reqBody.GetAddress()
	if addr == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(errMissingObjectAddress)))
	}

	cnrMsg := addr.GetContainerID()
	if cnrMsg == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidContainerIDError(errMissingContainerID))))
	}

	objIDMsg := addr.GetObjectID()
	if objIDMsg == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidObjectIDError(errMissingObjectID))))
	}

	var cnr cid.ID

	err = cnr.ReadFromV2(*cnrMsg)
	if err != nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidContainerIDError(err))))
	}

	var objID oid.ID

	err = objID.ReadFromV2(*objIDMsg)
	if err != nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidObjectIDError(err))))
	}

	bearerToken, err := bearerTokenFromRequest(req)
	if err != nil {
		return err
	}

	sessionToken, err := sessionTokenFromRequest(req)
	if err != nil {
		return err
	}

	r := aclservice.NewContainerRequest(cnr, bClientPubKey, aclservice.GetRequest(objID), wrapRequest(req))

	if bearerToken != nil {
		r.SetBearerToken(*bearerToken)
	}

	if sessionToken != nil {
		r.SetSessionToken(*sessionToken)
	}

	err = b.checker.CheckAccess(r)
	if err != nil {
		var errAccessDenied aclservice.AccessDeniedError

		switch {
		default:
			// here err may be protocol status and returned directly
			return err
		case errors.As(err, &errAccessDenied):
			var e apistatus.ObjectAccessDenied
			e.WriteReason(errAccessDenied.Error())

			return e
		case errors.Is(err, aclservice.ErrNotEnoughData):
			// response is needed, continue
		}
	}

	checkStream := newGetStreamWithAccessCheck(stream, b.checker, req, cnr, objID, bClientPubKey, sessionToken, bearerToken)

	return b.nextHandler.Get(req, checkStream)
}

// putStreamWithAccessCheck provides object write stream that performs access
// control: it handles object and request headers from incoming request
// messages.
type putStreamWithAccessCheck struct {
	// target stream
	object.PutObjectStream

	checker *aclservice.Checker

	// dynamic context

	processedInitialPart bool
}

func newPutStreamWithAccessCheck(targetStream object.PutObjectStream, checker *aclservice.Checker) *putStreamWithAccessCheck {
	return &putStreamWithAccessCheck{
		PutObjectStream: targetStream,
		checker:         checker,
	}
}

// Send intercepts and verifies a message carrying object headers by passing
// them to the underlying [aclservice.Checker]. Other messages and those that
// pass the check are forwarded to the underlying target stream.
func (x *putStreamWithAccessCheck) Send(req *objectV2.PutRequest) error {
	bClientPubKey, err := binaryClientPublicKeyFromRequest(req)
	if err != nil {
		return err
	}

	reqBody := req.GetBody()
	if reqBody == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(errMissingRequestBody))
	}

	initPart, ok := reqBody.GetObjectPart().(*objectV2.PutObjectPartInit)
	if !ok {
		return x.PutObjectStream.Send(req)
	}

	if x.processedInitialPart {
		return newInvalidRequestError(errMultipleStreamInitializers)
	}

	x.processedInitialPart = true

	hdrMsg := initPart.GetHeader()
	if hdrMsg == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(errMissingObjectHeader))
	}

	cnrMsg := hdrMsg.GetContainerID()
	if cnrMsg == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidContainerIDError(errMissingContainerID)))
	}

	var cnr cid.ID

	err = cnr.ReadFromV2(*cnrMsg)
	if err != nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidContainerIDError(err))))
	}

	bearerToken, err := bearerTokenFromRequest(req)
	if err != nil {
		return err
	}

	sessionToken, err := sessionTokenFromRequest(req)
	if err != nil {
		return err
	}

	// embed header into object message because objectSDK package doesn't work
	// with header type
	var objMsg objectV2.Object
	objMsg.SetHeader(hdrMsg)
	objMsg.SetObjectID(initPart.GetObjectID())
	objMsg.SetSignature(initPart.GetSignature())

	hdr := objectSDK.NewFromV2(&objMsg)

	r := aclservice.NewContainerRequest(cnr, bClientPubKey, aclservice.PutRequest(*hdr), wrapRequest(req))

	if bearerToken != nil {
		r.SetBearerToken(*bearerToken)
	}

	if sessionToken != nil {
		r.SetSessionToken(*sessionToken)
	}

	err = x.checker.CheckAccess(r)
	if err != nil {
		var errAccessDenied aclservice.AccessDeniedError

		switch {
		default:
			// here err may be protocol status and returned directly
			return err
		case errors.As(err, &errAccessDenied):
			var e apistatus.ObjectAccessDenied
			e.WriteReason(errAccessDenied.Error())

			return e
		case errors.Is(err, aclservice.ErrNotEnoughData):
			panic(fmt.Sprintf("unexpected error from ACL checker: %v", err))
		}
	}

	return x.PutObjectStream.Send(req)
}

func (b Service) Put(ctx context.Context) (object.PutObjectStream, error) {
	stream, err := b.nextHandler.Put(ctx)
	if err != nil {
		return nil, err
	}

	return newPutStreamWithAccessCheck(stream, b.checker), nil
}

func (b Service) Head(ctx context.Context, req *objectV2.HeadRequest) (*objectV2.HeadResponse, error) {
	bClientPubKey, err := binaryClientPublicKeyFromRequest(req)
	if err != nil {
		return nil, err
	}

	reqBody := req.GetBody()
	if reqBody == nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(errMissingRequestBody))
	}

	addr := reqBody.GetAddress()
	if addr == nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(errMissingObjectAddress)))
	}

	cnrMsg := addr.GetContainerID()
	if cnrMsg == nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidContainerIDError(errMissingContainerID))))
	}

	objIDMsg := addr.GetObjectID()
	if objIDMsg == nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidObjectIDError(errMissingObjectID))))
	}

	var cnr cid.ID

	err = cnr.ReadFromV2(*cnrMsg)
	if err != nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidContainerIDError(err))))
	}

	var objID oid.ID

	err = objID.ReadFromV2(*objIDMsg)
	if err != nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidObjectIDError(err))))
	}

	bearerToken, err := bearerTokenFromRequest(req)
	if err != nil {
		return nil, err
	}

	sessionToken, err := sessionTokenFromRequest(req)
	if err != nil {
		return nil, err
	}

	r := aclservice.NewContainerRequest(cnr, bClientPubKey, aclservice.HeadRequest(objID), wrapRequest(req))

	if bearerToken != nil {
		r.SetBearerToken(*bearerToken)
	}

	if sessionToken != nil {
		r.SetSessionToken(*sessionToken)
	}

	err = b.checker.CheckAccess(r)
	if err != nil {
		var errAccessDenied aclservice.AccessDeniedError

		switch {
		default:
			// here err may be protocol status and returned directly
			return nil, err
		case errors.As(err, &errAccessDenied):
			var e apistatus.ObjectAccessDenied
			e.WriteReason(errAccessDenied.Error())

			return nil, e
		case errors.Is(err, aclservice.ErrNotEnoughData):
			// response is needed, continue
		}
	}

	resp, err := b.nextHandler.Head(ctx, req)
	if err != nil {
		return nil, err
	}

	body := resp.GetBody()
	if body == nil {
		panic("missing response body")
	}

	var hdrMsg *objectV2.Header

	switch part := body.GetHeaderPart().(type) {
	default:
		panic(fmt.Sprintf("unexpected payload of the response %T", body.GetHeaderPart()))
	case *objectV2.SplitInfo:
		// just return response without object headers untouched
		return resp, nil
	case *objectV2.HeaderWithSignature:
		hdrMsg = part.GetHeader()
		if hdrMsg == nil {
			panic("missing object header in the response message")
		}
	case *objectV2.ShortHeader:
		hdrMsg = new(objectV2.Header)
		hdrMsg.SetVersion(part.GetVersion())
		hdrMsg.SetOwnerID(part.GetOwnerID())
		hdrMsg.SetObjectType(part.GetObjectType())
		hdrMsg.SetCreationEpoch(part.GetCreationEpoch())
		hdrMsg.SetPayloadLength(part.GetPayloadLength())
		hdrMsg.SetPayloadHash(part.GetPayloadHash())
		hdrMsg.SetHomomorphicHash(part.GetHomomorphicHash())
	}

	// embed header into object message because objectSDK package doesn't work
	// with header type
	var objMsg objectV2.Object
	objMsg.SetHeader(hdrMsg)

	hdr := objectSDK.NewFromV2(&objMsg)

	r = aclservice.NewContainerRequest(cnr, bClientPubKey, aclservice.HeadResponse(objID, *hdr), wrapRequest(req))

	if bearerToken != nil {
		r.SetBearerToken(*bearerToken)
	}

	if sessionToken != nil {
		r.SetSessionToken(*sessionToken)
	}

	err = b.checker.CheckAccess(r)
	if err != nil {
		var errAccessDenied aclservice.AccessDeniedError

		switch {
		default:
			// here err may be protocol status and returned directly
			return nil, err
		case errors.As(err, &errAccessDenied):
			var e apistatus.ObjectAccessDenied
			e.WriteReason(errAccessDenied.Error())

			return nil, e
		case errors.Is(err, aclservice.ErrNotEnoughData):
			panic(fmt.Sprintf("unexpected error from ACL checker: %v", err))
		}
	}

	return resp, nil
}

func (b Service) Search(req *objectV2.SearchRequest, stream object.SearchStream) error {
	bClientPubKey, err := binaryClientPublicKeyFromRequest(req)
	if err != nil {
		return err
	}

	reqBody := req.GetBody()
	if reqBody == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(errMissingRequestBody))
	}

	cnrMsg := reqBody.GetContainerID()
	if cnrMsg == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidContainerIDError(errMissingContainerID)))
	}

	var cnr cid.ID

	err = cnr.ReadFromV2(*cnrMsg)
	if err != nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidContainerIDError(err))))
	}

	bearerToken, err := bearerTokenFromRequest(req)
	if err != nil {
		return err
	}

	sessionToken, err := sessionTokenFromRequest(req)
	if err != nil {
		return err
	}

	r := aclservice.NewContainerRequest(cnr, bClientPubKey, aclservice.SearchRequest(), wrapRequest(req))

	if bearerToken != nil {
		r.SetBearerToken(*bearerToken)
	}

	if sessionToken != nil {
		r.SetSessionToken(*sessionToken)
	}

	err = b.checker.CheckAccess(r)
	if err != nil {
		var errAccessDenied aclservice.AccessDeniedError

		switch {
		default:
			// here err may be protocol status and returned directly
			return err
		case errors.As(err, &errAccessDenied):
			var e apistatus.ObjectAccessDenied
			e.WriteReason(errAccessDenied.Error())

			return e
		case errors.Is(err, aclservice.ErrNotEnoughData):
			panic(fmt.Sprintf("unexpected error from ACL checker: %v", err))
		}
	}

	return b.nextHandler.Search(req, stream)
}

func (b Service) Delete(ctx context.Context, req *objectV2.DeleteRequest) (*objectV2.DeleteResponse, error) {
	bClientPubKey, err := binaryClientPublicKeyFromRequest(req)
	if err != nil {
		return nil, err
	}

	reqBody := req.GetBody()
	if reqBody == nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(errMissingRequestBody))
	}

	addr := reqBody.GetAddress()
	if addr == nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(errMissingObjectAddress)))
	}

	cnrMsg := addr.GetContainerID()
	if cnrMsg == nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidContainerIDError(errMissingContainerID))))
	}

	objIDMsg := addr.GetObjectID()
	if objIDMsg == nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidObjectIDError(errMissingObjectID))))
	}

	var cnr cid.ID

	err = cnr.ReadFromV2(*cnrMsg)
	if err != nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidContainerIDError(err))))
	}

	var objID oid.ID

	err = objID.ReadFromV2(*objIDMsg)
	if err != nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidObjectIDError(err))))
	}

	bearerToken, err := bearerTokenFromRequest(req)
	if err != nil {
		return nil, err
	}

	sessionToken, err := sessionTokenFromRequest(req)
	if err != nil {
		return nil, err
	}

	r := aclservice.NewContainerRequest(cnr, bClientPubKey, aclservice.DeleteRequest(objID), wrapRequest(req))

	if bearerToken != nil {
		r.SetBearerToken(*bearerToken)
	}

	if sessionToken != nil {
		r.SetSessionToken(*sessionToken)
	}

	err = b.checker.CheckAccess(r)
	if err != nil {
		var errAccessDenied aclservice.AccessDeniedError

		switch {
		default:
			// here err may be protocol status and returned directly
			return nil, err
		case errors.As(err, &errAccessDenied):
			var e apistatus.ObjectAccessDenied
			e.WriteReason(errAccessDenied.Error())

			return nil, e
		case errors.Is(err, aclservice.ErrNotEnoughData):
			panic(fmt.Sprintf("unexpected error from ACL checker: %v", err))
		}
	}

	return b.nextHandler.Delete(ctx, req)
}

func (b Service) GetRange(req *objectV2.GetRangeRequest, stream object.GetObjectRangeStream) error {
	bClientPubKey, err := binaryClientPublicKeyFromRequest(req)
	if err != nil {
		return err
	}

	reqBody := req.GetBody()
	if reqBody == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(errMissingRequestBody))
	}

	addr := reqBody.GetAddress()
	if addr == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(errMissingObjectAddress)))
	}

	cnrMsg := addr.GetContainerID()
	if cnrMsg == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidContainerIDError(errMissingContainerID))))
	}

	objIDMsg := addr.GetObjectID()
	if objIDMsg == nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidObjectIDError(errMissingObjectID))))
	}

	var cnr cid.ID

	err = cnr.ReadFromV2(*cnrMsg)
	if err != nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidContainerIDError(err))))
	}

	var objID oid.ID

	err = objID.ReadFromV2(*objIDMsg)
	if err != nil {
		return newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidObjectIDError(err))))
	}

	bearerToken, err := bearerTokenFromRequest(req)
	if err != nil {
		return err
	}

	sessionToken, err := sessionTokenFromRequest(req)
	if err != nil {
		return err
	}

	r := aclservice.NewContainerRequest(cnr, bClientPubKey, aclservice.PayloadRangeRequest(objID), wrapRequest(req))

	if bearerToken != nil {
		r.SetBearerToken(*bearerToken)
	}

	if sessionToken != nil {
		r.SetSessionToken(*sessionToken)
	}

	err = b.checker.CheckAccess(r)
	if err != nil {
		var errAccessDenied aclservice.AccessDeniedError

		switch {
		default:
			// here err may be protocol status and returned directly
			return err
		case errors.As(err, &errAccessDenied):
			var e apistatus.ObjectAccessDenied
			e.WriteReason(errAccessDenied.Error())

			return e
		case errors.Is(err, aclservice.ErrNotEnoughData):
			panic(fmt.Sprintf("unexpected error from ACL checker: %v", err))
		}
	}

	return b.nextHandler.GetRange(req, stream)
}

func (b Service) GetRangeHash(ctx context.Context, req *objectV2.GetRangeHashRequest) (*objectV2.GetRangeHashResponse, error) {
	bClientPubKey, err := binaryClientPublicKeyFromRequest(req)
	if err != nil {
		return nil, err
	}

	reqBody := req.GetBody()
	if reqBody == nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(errMissingRequestBody))
	}

	addr := reqBody.GetAddress()
	if addr == nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(errMissingObjectAddress)))
	}

	cnrMsg := addr.GetContainerID()
	if cnrMsg == nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidContainerIDError(errMissingContainerID))))
	}

	objIDMsg := addr.GetObjectID()
	if objIDMsg == nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidObjectIDError(errMissingObjectID))))
	}

	var cnr cid.ID

	err = cnr.ReadFromV2(*cnrMsg)
	if err != nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidContainerIDError(err))))
	}

	var objID oid.ID

	err = objID.ReadFromV2(*objIDMsg)
	if err != nil {
		return nil, newInvalidRequestError(
			newInvalidRequestBodyError(
				newInvalidObjectAddressError(
					newInvalidObjectIDError(err))))
	}

	bearerToken, err := bearerTokenFromRequest(req)
	if err != nil {
		return nil, err
	}

	sessionToken, err := sessionTokenFromRequest(req)
	if err != nil {
		return nil, err
	}

	r := aclservice.NewContainerRequest(cnr, bClientPubKey, aclservice.HashPayloadRangeRequest(objID), wrapRequest(req))

	if bearerToken != nil {
		r.SetBearerToken(*bearerToken)
	}

	if sessionToken != nil {
		r.SetSessionToken(*sessionToken)
	}

	err = b.checker.CheckAccess(r)
	if err != nil {
		var errAccessDenied aclservice.AccessDeniedError

		switch {
		default:
			// here err may be protocol status and returned directly
			return nil, err
		case errors.As(err, &errAccessDenied):
			var e apistatus.ObjectAccessDenied
			e.WriteReason(errAccessDenied.Error())

			return nil, e
		case errors.Is(err, aclservice.ErrNotEnoughData):
			panic(fmt.Sprintf("unexpected error from ACL checker: %v", err))
		}
	}

	return b.nextHandler.GetRangeHash(ctx, req)
}
