package v2

import (
	"context"
	"errors"
	"fmt"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
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

	panicOnNil := func(v interface{}, name string) {
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

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return err
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

	reqInfo.obj, err = getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return err
	}

	useObjectIDFromSession(&reqInfo, sTok)

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

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return nil, err
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

	reqInfo.obj, err = getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return nil, err
	}

	useObjectIDFromSession(&reqInfo, sTok)

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

	reqInfo.obj, err = getObjectIDFromRequestBody(request.GetBody())
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

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return nil, err
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

	reqInfo.obj, err = getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return nil, err
	}

	useObjectIDFromSession(&reqInfo, sTok)

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

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return err
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

	reqInfo.obj, err = getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return err
	}
	useObjectIDFromSession(&reqInfo, sTok)

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

	sTok, err := originalSessionToken(request.GetMetaHeader())
	if err != nil {
		return nil, err
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

	reqInfo.obj, err = getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return nil, err
	}

	useObjectIDFromSession(&reqInfo, sTok)

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
		return ErrMalformedRequest
	}

	part := body.GetObjectPart()
	if part, ok := part.(*objectV2.PutObjectPartInit); ok {
		cnr, err := getContainerIDFromRequest(request)
		if err != nil {
			return err
		}

		idV2 := part.GetHeader().GetOwnerID()
		if idV2 == nil {
			return errors.New("missing object owner")
		}

		var idOwner user.ID

		err = idOwner.ReadFromV2(*idV2)
		if err != nil {
			return fmt.Errorf("invalid object owner: %w", err)
		}

		var sTok *sessionSDK.Object

		if tokV2 := request.GetMetaHeader().GetSessionToken(); tokV2 != nil {
			sTok = new(sessionSDK.Object)

			err = sTok.ReadFromV2(*tokV2)
			if err != nil {
				return fmt.Errorf("invalid session token: %w", err)
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

		reqInfo, err := p.source.findRequestInfo(req, cnr, acl.OpObjectPut)
		if err != nil {
			return err
		}

		reqInfo.obj, err = getObjectIDFromRequestBody(part)
		if err != nil {
			return err
		}

		useObjectIDFromSession(&reqInfo, sTok)

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
			return info, fmt.Errorf("%w: token has expired (current epoch: %d)",
				ErrMalformedRequest, currentEpoch)
		}

		if !assertVerb(*req.token, op) {
			return info, ErrInvalidVerb
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
