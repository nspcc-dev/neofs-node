package v2

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

// Service checks basic ACL rules.
type Service struct {
	*cfg

	c senderClassifier
}

// Option represents Service constructor option.
type Option func(*cfg)

// Netmapper must provide network map information.
type Netmapper interface {
	netmap.Source
	// ServerInContainer checks if current node belongs to requested container.
	// Any unknown state must be returned as `(false, error.New("explanation"))`,
	// not `(false, nil)`.
	ServerInContainer(cid.ID) (bool, error)
}

type cfg struct {
	log *zap.Logger

	containers container.Source

	irFetcher InnerRingFetcher

	nm Netmapper
}

func defaultCfg() *cfg {
	return &cfg{
		log: zap.L(),
	}
}

// New is a constructor for object ACL checking service.
func New(opts ...Option) Service {
	cfg := defaultCfg()

	for i := range opts {
		opts[i](cfg)
	}

	panicOnNil := func(v any, name string) {
		if v == nil {
			panic(fmt.Sprintf("ACL service: %s is nil", name))
		}
	}

	panicOnNil(cfg.nm, "netmap client")
	panicOnNil(cfg.irFetcher, "inner Ring fetcher")
	panicOnNil(cfg.containers, "container source")

	return Service{
		cfg: cfg,
		c: senderClassifier{
			log:       cfg.log,
			innerRing: cfg.irFetcher,
			netmap:    cfg.nm,
		},
	}
}

// GetRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) GetRequestToInfo(request *protoobject.GetRequest) (RequestInfo, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return RequestInfo{}, err
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, err
	}

	if sTok != nil {
		err = assertSessionRelation(*sTok, cnr, obj)
		if err != nil {
			return RequestInfo{}, err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, err
	}

	req := MetaWithToken{
		vheader: request.GetVerifyHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cnr, acl.OpObjectGet)
	if err != nil {
		return RequestInfo{}, err
	}

	reqInfo.obj = obj

	return reqInfo, nil
}

// HeadRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) HeadRequestToInfo(request *protoobject.HeadRequest) (RequestInfo, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return RequestInfo{}, err
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, err
	}

	if sTok != nil {
		err = assertSessionRelation(*sTok, cnr, obj)
		if err != nil {
			return RequestInfo{}, err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, err
	}

	req := MetaWithToken{
		vheader: request.GetVerifyHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cnr, acl.OpObjectHead)
	if err != nil {
		return RequestInfo{}, err
	}

	reqInfo.obj = obj

	return reqInfo, err
}

// SearchRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) SearchRequestToInfo(request *protoobject.SearchRequest) (RequestInfo, error) {
	return b.searchRequestToInfo(request)
}

// SearchV2RequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) SearchV2RequestToInfo(request *protoobject.SearchV2Request) (RequestInfo, error) {
	return b.searchRequestToInfo(request)
}

// unifies V1 and V2 search request processing.
func (b Service) searchRequestToInfo(request interface {
	GetMetaHeader() *protosession.RequestMetaHeader
	GetVerifyHeader() *protosession.RequestVerificationHeader
}) (RequestInfo, error) {
	id, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, err
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, err
	}

	if sTok != nil {
		err = assertSessionRelation(*sTok, id, nil)
		if err != nil {
			return RequestInfo{}, err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, err
	}

	req := MetaWithToken{
		vheader: request.GetVerifyHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, id, acl.OpObjectSearch)
	if err != nil {
		return RequestInfo{}, err
	}

	return reqInfo, nil
}

// DeleteRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) DeleteRequestToInfo(request *protoobject.DeleteRequest) (RequestInfo, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return RequestInfo{}, err
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, err
	}

	if sTok != nil {
		err = assertSessionRelation(*sTok, cnr, obj)
		if err != nil {
			return RequestInfo{}, err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, err
	}

	req := MetaWithToken{
		vheader: request.GetVerifyHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cnr, acl.OpObjectDelete)
	if err != nil {
		return RequestInfo{}, err
	}

	reqInfo.obj = obj

	return reqInfo, nil
}

// RangeRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) RangeRequestToInfo(request *protoobject.GetRangeRequest) (RequestInfo, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return RequestInfo{}, err
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, err
	}

	if sTok != nil {
		err = assertSessionRelation(*sTok, cnr, obj)
		if err != nil {
			return RequestInfo{}, err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, err
	}

	req := MetaWithToken{
		vheader: request.GetVerifyHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cnr, acl.OpObjectRange)
	if err != nil {
		return RequestInfo{}, err
	}

	reqInfo.obj = obj

	return reqInfo, nil
}

// HashRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) HashRequestToInfo(request *protoobject.GetRangeHashRequest) (RequestInfo, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return RequestInfo{}, err
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, err
	}

	if sTok != nil {
		err = assertSessionRelation(*sTok, cnr, obj)
		if err != nil {
			return RequestInfo{}, err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, err
	}

	req := MetaWithToken{
		vheader: request.GetVerifyHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cnr, acl.OpObjectHash)
	if err != nil {
		return RequestInfo{}, err
	}

	reqInfo.obj = obj

	return reqInfo, nil
}

var ErrSkipRequest = errors.New("skip request")

// PutRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker]. Returns [ErrSkipRequest] if check should not be performed.
func (b Service) PutRequestToInfo(request *protoobject.PutRequest) (RequestInfo, user.ID, error) {
	body := request.GetBody()
	if body == nil {
		return RequestInfo{}, user.ID{}, errEmptyBody
	}

	part, ok := body.GetObjectPart().(*protoobject.PutRequest_Body_Init_)
	if !ok {
		return RequestInfo{}, user.ID{}, ErrSkipRequest
	}
	if part == nil || part.Init == nil {
		return RequestInfo{}, user.ID{}, errors.New("nil oneof field with heading part")
	}
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, user.ID{}, err
	}

	header := part.Init.Header

	cIDV2 := header.GetContainerId().GetValue()
	var cID cid.ID
	err = cID.Decode(cIDV2)
	if err != nil {
		return RequestInfo{}, user.ID{}, fmt.Errorf("invalid container ID: %w", err)
	}

	inContainer, err := b.nm.ServerInContainer(cID)
	if err != nil {
		return RequestInfo{}, user.ID{}, fmt.Errorf("checking if node in container: %w", err)
	}

	if header.GetSplit() != nil && !inContainer {
		// skip ACL checks for split objects if it is not a container
		// node, since it requires additional object operations (e.g.,
		// requesting the other split parts) that may (or may not) be
		// prohibited by ACL rules; this node is not going to store such
		// objects anyway
		return RequestInfo{}, user.ID{}, ErrSkipRequest
	}

	mOwner := header.GetOwnerId()
	if mOwner == nil {
		return RequestInfo{}, user.ID{}, errors.New("missing object owner")
	}

	var idOwner user.ID
	err = idOwner.FromProtoMessage(mOwner)
	if err != nil {
		return RequestInfo{}, user.ID{}, fmt.Errorf("invalid object owner: %w", err)
	}

	var obj *oid.ID

	if part.Init.ObjectId != nil {
		obj = new(oid.ID)
		err = obj.FromProtoMessage(part.Init.ObjectId)
		if err != nil {
			return RequestInfo{}, user.ID{}, err
		}
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, user.ID{}, err
	}

	if sTok != nil {
		if sTok.AssertVerb(sessionSDK.VerbObjectDelete) {
			// if session relates to object's removal, we don't check
			// relation of the tombstone to the session here since user
			// can't predict tomb's ID.
			err = assertSessionRelation(*sTok, cnr, nil)
		} else {
			err = assertSessionRelation(*sTok, cnr, obj)
		}

		if err != nil {
			return RequestInfo{}, user.ID{}, err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return RequestInfo{}, user.ID{}, err
	}

	req := MetaWithToken{
		vheader: request.GetVerifyHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	verb := acl.OpObjectPut
	tombstone := header.GetObjectType() == protoobject.ObjectType_TOMBSTONE
	if tombstone {
		// such objects are specific - saving them is essentially the removal of other
		// objects
		verb = acl.OpObjectDelete
	}

	reqInfo, err := b.findRequestInfo(req, cnr, verb)
	if err != nil {
		return RequestInfo{}, user.ID{}, err
	}

	replication := reqInfo.requestRole == acl.RoleContainer && request.GetMetaHeader().GetTtl() == 1
	if tombstone {
		// the only exception when writing tombstone should not be treated as deletion
		// is intra-container replication: container nodes must be able to replicate
		// such objects while deleting is prohibited
		if replication {
			reqInfo.operation = acl.OpObjectPut
		}
	}

	if !replication {
		// header length is unchecked for replication because introducing a restriction
		// should not prevent the replication of objects created before.
		// See also https://github.com/nspcc-dev/neofs-api/issues/293
		var hdrLen = header.MarshaledSize()
		if hdrLen > objectsdk.MaxHeaderLen {
			return RequestInfo{}, user.ID{}, fmt.Errorf("object header length exceeds the limit: %d>%d", hdrLen, objectsdk.MaxHeaderLen)
		}
	}

	reqInfo.obj = obj

	return reqInfo, idOwner, nil
}

func (b Service) findRequestInfo(req MetaWithToken, idCnr cid.ID, op acl.Op) (info RequestInfo, err error) {
	cnr, err := b.containers.Get(idCnr) // fetch actual container
	if err != nil {
		return info, err
	}

	currentEpoch, err := b.nm.Epoch()
	if err != nil {
		return info, errors.New("can't fetch current epoch")
	}
	if req.token != nil {
		if req.token.ExpiredAt(currentEpoch) {
			return info, apistatus.SessionTokenExpired{}
		}
		if !req.token.ValidAt(currentEpoch) {
			return info, fmt.Errorf("%s: token is invalid at %d epoch)",
				invalidRequestMessage, currentEpoch)
		}

		if !assertVerb(*req.token, op) {
			return info, errInvalidVerb
		}
	}

	// find request role and key
	res, err := b.c.classify(req, idCnr, cnr.Value, currentEpoch)
	if err != nil {
		return info, err
	}

	info.basicACL = cnr.Value.BasicACL()
	info.requestRole = res.role
	info.operation = op
	info.cnrOwner = cnr.Value.Owner()
	info.idCnr = idCnr

	// it is assumed that at the moment the key will be valid,
	// otherwise the request would not pass validation
	info.senderKey = res.key
	info.senderAccount = res.account

	// add bearer token if it is present in request
	info.bearer = req.bearer

	info.srcRequest = req.src

	return info, nil
}
