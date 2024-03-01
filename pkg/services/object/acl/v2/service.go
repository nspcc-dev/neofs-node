package v2

import (
	"context"
	"errors"
	"fmt"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/object"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

// Service checks basic ACL rules.
type Service struct {
	*cfg

	c senderClassifier
}

type putStreamBasicChecker struct {
	source *Service
	next   object.PutObjectStream
}

type getStreamBasicChecker struct {
	checker ACLChecker

	object.GetObjectStream

	info RequestInfo
}

type rangeStreamBasicChecker struct {
	checker ACLChecker

	object.GetObjectRangeStream

	info RequestInfo
}

type searchStreamBasicChecker struct {
	checker ACLChecker

	object.SearchStream

	info RequestInfo
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	log *zap.Logger

	containers container.Source

	checker ACLChecker

	irFetcher InnerRingFetcher

	nm netmap.Source

	next object.ServiceServer
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

	panicOnNil(cfg.next, "next Service")
	panicOnNil(cfg.nm, "netmap client")
	panicOnNil(cfg.irFetcher, "inner Ring fetcher")
	panicOnNil(cfg.checker, "acl checker")
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

// Get implements ServiceServer interface, makes ACL checks and calls
// next Get method in the ServiceServer pipeline.
func (b Service) Get(request *objectV2.GetRequest, stream object.GetObjectStream) error {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return err
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return err
	}

	if sTok != nil {
		err = assertSessionRelation(*sTok, cnr, obj)
		if err != nil {
			return err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return err
	}

	req := MetaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cnr, acl.OpObjectGet)
	if err != nil {
		return err
	}

	reqInfo.obj = obj

	if !b.checker.CheckBasicACL(reqInfo) {
		return basicACLErr(reqInfo)
	} else if err := b.checker.CheckEACL(request, reqInfo); err != nil {
		return eACLErr(reqInfo, err)
	}

	return b.next.Get(request, &getStreamBasicChecker{
		GetObjectStream: stream,
		info:            reqInfo,
		checker:         b.checker,
	})
}

func (b Service) Put(ctx context.Context) (object.PutObjectStream, error) {
	streamer, err := b.next.Put(ctx)

	return putStreamBasicChecker{
		source: &b,
		next:   streamer,
	}, err
}

func (b Service) Head(
	ctx context.Context,
	request *objectV2.HeadRequest) (*objectV2.HeadResponse, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return nil, err
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return nil, err
	}

	if sTok != nil {
		err = assertSessionRelation(*sTok, cnr, obj)
		if err != nil {
			return nil, err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return nil, err
	}

	req := MetaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cnr, acl.OpObjectHead)
	if err != nil {
		return nil, err
	}

	reqInfo.obj = obj

	if !b.checker.CheckBasicACL(reqInfo) {
		return nil, basicACLErr(reqInfo)
	} else if err := b.checker.CheckEACL(request, reqInfo); err != nil {
		return nil, eACLErr(reqInfo, err)
	}

	resp, err := b.next.Head(ctx, request)
	if err == nil {
		if err = b.checker.CheckEACL(resp, reqInfo); err != nil {
			err = eACLErr(reqInfo, err)
		}
	}

	return resp, err
}

func (b Service) Search(request *objectV2.SearchRequest, stream object.SearchStream) error {
	id, err := getContainerIDFromRequest(request)
	if err != nil {
		return err
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return err
	}

	if sTok != nil {
		err = assertSessionRelation(*sTok, id, nil)
		if err != nil {
			return err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return err
	}

	req := MetaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, id, acl.OpObjectSearch)
	if err != nil {
		return err
	}

	if !b.checker.CheckBasicACL(reqInfo) {
		return basicACLErr(reqInfo)
	} else if err := b.checker.CheckEACL(request, reqInfo); err != nil {
		return eACLErr(reqInfo, err)
	}

	return b.next.Search(request, &searchStreamBasicChecker{
		checker:      b.checker,
		SearchStream: stream,
		info:         reqInfo,
	})
}

func (b Service) Delete(
	ctx context.Context,
	request *objectV2.DeleteRequest) (*objectV2.DeleteResponse, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return nil, err
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return nil, err
	}

	if sTok != nil {
		err = assertSessionRelation(*sTok, cnr, obj)
		if err != nil {
			return nil, err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return nil, err
	}

	req := MetaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cnr, acl.OpObjectDelete)
	if err != nil {
		return nil, err
	}

	reqInfo.obj = obj

	if !b.checker.CheckBasicACL(reqInfo) {
		return nil, basicACLErr(reqInfo)
	} else if err := b.checker.CheckEACL(request, reqInfo); err != nil {
		return nil, eACLErr(reqInfo, err)
	}

	return b.next.Delete(ctx, request)
}

func (b Service) GetRange(request *objectV2.GetRangeRequest, stream object.GetObjectRangeStream) error {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return err
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return err
	}

	if sTok != nil {
		err = assertSessionRelation(*sTok, cnr, obj)
		if err != nil {
			return err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return err
	}

	req := MetaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cnr, acl.OpObjectRange)
	if err != nil {
		return err
	}

	reqInfo.obj = obj

	if !b.checker.CheckBasicACL(reqInfo) {
		return basicACLErr(reqInfo)
	} else if err := b.checker.CheckEACL(request, reqInfo); err != nil {
		return eACLErr(reqInfo, err)
	}

	return b.next.GetRange(request, &rangeStreamBasicChecker{
		checker:              b.checker,
		GetObjectRangeStream: stream,
		info:                 reqInfo,
	})
}

func (b Service) GetRangeHash(
	ctx context.Context,
	request *objectV2.GetRangeHashRequest) (*objectV2.GetRangeHashResponse, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return nil, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return nil, err
	}

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return nil, err
	}

	if sTok != nil {
		err = assertSessionRelation(*sTok, cnr, obj)
		if err != nil {
			return nil, err
		}
	}

	bTok, err := originalBearerToken(request.GetMetaHeader())
	if err != nil {
		return nil, err
	}

	req := MetaWithToken{
		vheader: request.GetVerificationHeader(),
		token:   sTok,
		bearer:  bTok,
		src:     request,
	}

	reqInfo, err := b.findRequestInfo(req, cnr, acl.OpObjectHash)
	if err != nil {
		return nil, err
	}

	reqInfo.obj = obj

	if !b.checker.CheckBasicACL(reqInfo) {
		return nil, basicACLErr(reqInfo)
	} else if err := b.checker.CheckEACL(request, reqInfo); err != nil {
		return nil, eACLErr(reqInfo, err)
	}

	return b.next.GetRangeHash(ctx, request)
}

func (p putStreamBasicChecker) Send(request *objectV2.PutRequest) error {
	body := request.GetBody()
	if body == nil {
		return errEmptyBody
	}

	part := body.GetObjectPart()
	if part, ok := part.(*objectV2.PutObjectPartInit); ok {
		cnr, err := getContainerIDFromRequest(request)
		if err != nil {
			return err
		}

		header := part.GetHeader()

		idV2 := header.GetOwnerID()
		if idV2 == nil {
			return errors.New("missing object owner")
		}

		var idOwner user.ID

		err = idOwner.ReadFromV2(*idV2)
		if err != nil {
			return fmt.Errorf("invalid object owner: %w", err)
		}

		objV2 := part.GetObjectID()
		var obj *oid.ID

		if objV2 != nil {
			obj = new(oid.ID)

			err = obj.ReadFromV2(*objV2)
			if err != nil {
				return err
			}
		}

		sTok, err := originalSessionToken(request.GetMetaHeader())
		if err != nil {
			return err
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
				return err
			}
		}

		bTok, err := originalBearerToken(request.GetMetaHeader())
		if err != nil {
			return err
		}

		req := MetaWithToken{
			vheader: request.GetVerificationHeader(),
			token:   sTok,
			bearer:  bTok,
			src:     request,
		}

		verb := acl.OpObjectPut
		tombstone := part.GetHeader().GetObjectType() == objectV2.TypeTombstone
		if tombstone {
			// such objects are specific - saving them is essentially the removal of other
			// objects
			verb = acl.OpObjectDelete
		}

		reqInfo, err := p.source.findRequestInfo(req, cnr, verb)
		if err != nil {
			return err
		}

		replication := reqInfo.requestRole == acl.RoleContainer && request.GetMetaHeader().GetTTL() == 1
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
			hdrLen := part.GetHeader().StableSize()
			if hdrLen > objectsdk.MaxHeaderLen {
				return fmt.Errorf("object header length exceeds the limit: %d>%d", hdrLen, objectsdk.MaxHeaderLen)
			}
		}

		reqInfo.obj = obj

		if !p.source.checker.CheckBasicACL(reqInfo) || !p.source.checker.StickyBitCheck(reqInfo, idOwner) {
			return basicACLErr(reqInfo)
		} else if err := p.source.checker.CheckEACL(request, reqInfo); err != nil {
			return eACLErr(reqInfo, err)
		}
	}

	return p.next.Send(request)
}

func (p putStreamBasicChecker) CloseAndRecv() (*objectV2.PutResponse, error) {
	return p.next.CloseAndRecv()
}

func (g *getStreamBasicChecker) Send(resp *objectV2.GetResponse) error {
	if _, ok := resp.GetBody().GetObjectPart().(*objectV2.GetObjectPartInit); ok {
		if err := g.checker.CheckEACL(resp, g.info); err != nil {
			return eACLErr(g.info, err)
		}
	}

	return g.GetObjectStream.Send(resp)
}

func (g *rangeStreamBasicChecker) Send(resp *objectV2.GetRangeResponse) error {
	if err := g.checker.CheckEACL(resp, g.info); err != nil {
		return eACLErr(g.info, err)
	}

	return g.GetObjectRangeStream.Send(resp)
}

func (g *searchStreamBasicChecker) Send(resp *objectV2.SearchResponse) error {
	if err := g.checker.CheckEACL(resp, g.info); err != nil {
		return eACLErr(g.info, err)
	}

	return g.SearchStream.Send(resp)
}

func (b Service) findRequestInfo(req MetaWithToken, idCnr cid.ID, op acl.Op) (info RequestInfo, err error) {
	cnr, err := b.containers.Get(idCnr) // fetch actual container
	if err != nil {
		return info, err
	}

	if req.token != nil {
		currentEpoch, err := b.nm.Epoch()
		if err != nil {
			return info, errors.New("can't fetch current epoch")
		}
		if req.token.ExpiredAt(currentEpoch) {
			return info, apistatus.SessionTokenExpired{}
		}
		if req.token.InvalidAt(currentEpoch) {
			return info, fmt.Errorf("%s: token is invalid at %d epoch)",
				invalidRequestMessage, currentEpoch)
		}

		if !assertVerb(*req.token, op) {
			return info, errInvalidVerb
		}
	}

	// find request role and key
	res, err := b.c.classify(req, idCnr, cnr.Value)
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

	// add bearer token if it is present in request
	info.bearer = req.bearer

	info.srcRequest = req.src

	return info, nil
}
