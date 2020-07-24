package object

import (
	"context"

	eacl "github.com/nspcc-dev/neofs-api-go/acl/extended"
	"github.com/nspcc-dev/neofs-api-go/object"
	eaclstorage "github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended/storage"
)

type (
	serviceResponse interface {
		SetEpoch(uint64)
	}

	responsePreparer interface {
		prepareResponse(context.Context, serviceRequest, serviceResponse) error
	}

	epochResponsePreparer struct {
		epochRecv EpochReceiver
	}
)

type complexResponsePreparer struct {
	items []responsePreparer
}

type aclResponsePreparer struct {
	eaclSrc eaclstorage.Storage

	aclInfoReceiver aclInfoReceiver

	reqActCalc requestActionCalculator
}

type headersFromObject struct {
	obj *Object
}

var (
	_ responsePreparer = (*epochResponsePreparer)(nil)
)

func (s headersFromObject) getHeaders() (*Object, bool) {
	return s.obj, true
}

func (s complexResponsePreparer) prepareResponse(ctx context.Context, req serviceRequest, resp serviceResponse) error {
	for i := range s.items {
		if err := s.items[i].prepareResponse(ctx, req, resp); err != nil {
			return err
		}
	}

	return nil
}

func (s *epochResponsePreparer) prepareResponse(_ context.Context, req serviceRequest, resp serviceResponse) error {
	resp.SetEpoch(s.epochRecv.Epoch())

	return nil
}

func (s *aclResponsePreparer) prepareResponse(ctx context.Context, req serviceRequest, resp serviceResponse) error {
	aclInfo, err := s.aclInfoReceiver.getACLInfo(ctx, req)
	if err != nil {
		return errAccessDenied
	} else if !aclInfo.checkBearer && !aclInfo.checkExtended {
		return nil
	}

	var obj *Object

	switch r := resp.(type) {
	case *object.GetResponse:
		obj = r.GetObject()
	case *object.HeadResponse:
		obj = r.GetObject()
	case interface {
		GetObject() *Object
	}:
		obj = r.GetObject()
	}

	if obj == nil {
		return nil
	}

	// FIXME: do not check request headers.
	//  At this stage request is already validated, but action calculator will check it again.
	p := requestActionParams{
		eaclSrc: s.eaclSrc,
		request: req,
		objHdrSrc: headersFromObject{
			obj: obj,
		},
		group: aclInfo.targetInfo.group,
	}

	if aclInfo.checkBearer {
		p.eaclSrc = eaclFromBearer{
			bearer: req.GetBearerToken(),
		}
	}

	if action := s.reqActCalc.calculateRequestAction(ctx, p); action != eacl.ActionAllow {
		return errAccessDenied
	}

	return nil
}

func makeDeleteResponse() *object.DeleteResponse {
	return new(object.DeleteResponse)
}

func makeRangeHashResponse(v []Hash) *GetRangeHashResponse {
	return &GetRangeHashResponse{Hashes: v}
}

func makeRangeResponse(v []byte) *GetRangeResponse {
	return &GetRangeResponse{Fragment: v}
}

func makeSearchResponse(v []Address) *object.SearchResponse {
	return &object.SearchResponse{Addresses: v}
}

func makeHeadResponse(v *Object) *object.HeadResponse {
	return &object.HeadResponse{Object: v}
}

func makePutResponse(v Address) *object.PutResponse {
	return &object.PutResponse{Address: v}
}

func makeGetHeaderResponse(v *Object) *object.GetResponse {
	return &object.GetResponse{R: &object.GetResponse_Object{Object: v}}
}

func makeGetChunkResponse(v []byte) *object.GetResponse {
	return &object.GetResponse{R: &object.GetResponse_Chunk{Chunk: v}}
}
