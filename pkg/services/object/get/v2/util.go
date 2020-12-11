package getsvc

import (
	"crypto/sha256"
	"hash"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/pkg/errors"
)

func (s *Service) toPrm(req *objectV2.GetRequest, stream objectSvc.GetObjectStream) (*getsvc.Prm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(token.NewSessionTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	p := new(getsvc.Prm)
	p.SetCommonParameters(util.CommonPrmFromV2(req).
		WithPrivateKey(key),
	)

	body := req.GetBody()
	p.WithAddress(objectSDK.NewAddressFromV2(body.GetAddress()))
	p.WithRawFlag(body.GetRaw())
	p.SetObjectWriter(&streamObjectWriter{stream})

	return p, nil
}

func (s *Service) toRangePrm(req *objectV2.GetRangeRequest, stream objectSvc.GetObjectRangeStream) (*getsvc.RangePrm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(token.NewSessionTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	p := new(getsvc.RangePrm)
	p.SetCommonParameters(util.CommonPrmFromV2(req).
		WithPrivateKey(key),
	)

	body := req.GetBody()
	p.WithAddress(objectSDK.NewAddressFromV2(body.GetAddress()))
	p.WithRawFlag(body.GetRaw())
	p.SetChunkWriter(&streamObjectRangeWriter{stream})
	p.SetRange(objectSDK.NewRangeFromV2(body.GetRange()))

	return p, nil
}

func (s *Service) toHashRangePrm(req *objectV2.GetRangeHashRequest) (*getsvc.RangeHashPrm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(token.NewSessionTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	p := new(getsvc.RangeHashPrm)
	p.SetCommonParameters(util.CommonPrmFromV2(req).
		WithPrivateKey(key),
	)

	body := req.GetBody()
	p.WithAddress(objectSDK.NewAddressFromV2(body.GetAddress()))

	rngsV2 := body.GetRanges()
	rngs := make([]*objectSDK.Range, 0, len(rngsV2))

	for i := range rngsV2 {
		rngs = append(rngs, objectSDK.NewRangeFromV2(rngsV2[i]))
	}

	p.SetRangeList(rngs)

	switch t := body.GetType(); t {
	default:
		return nil, errors.Errorf("unknown checksum type %v", t)
	case refs.SHA256:
		p.SetHashGenerator(func() hash.Hash {
			return sha256.New()
		})
	case refs.TillichZemor:
		p.SetHashGenerator(func() hash.Hash {
			return tz.New()
		})
	}

	return p, nil
}

type headResponseWriter struct {
	mainOnly bool

	body *objectV2.HeadResponseBody
}

func (w *headResponseWriter) WriteHeader(hdr *object.Object) error {
	if w.mainOnly {
		w.body.SetHeaderPart(toShortObjectHeader(hdr))
	} else {
		w.body.SetHeaderPart(toFullObjectHeader(hdr))
	}

	return nil
}

func (s *Service) toHeadPrm(req *objectV2.HeadRequest, resp *objectV2.HeadResponse) (*getsvc.HeadPrm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(token.NewSessionTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	p := new(getsvc.HeadPrm)
	p.SetCommonParameters(util.CommonPrmFromV2(req).
		WithPrivateKey(key),
	)

	body := req.GetBody()
	p.WithAddress(objectSDK.NewAddressFromV2(body.GetAddress()))
	p.WithRawFlag(body.GetRaw())
	p.SetHeaderWriter(&headResponseWriter{
		mainOnly: body.GetMainOnly(),
		body:     resp.GetBody(),
	})

	return p, nil
}

func splitInfoResponse(info *objectSDK.SplitInfo) *objectV2.GetResponse {
	resp := new(objectV2.GetResponse)

	body := new(objectV2.GetResponseBody)
	resp.SetBody(body)

	body.SetObjectPart(info.ToV2())

	return resp
}

func splitInfoRangeResponse(info *objectSDK.SplitInfo) *objectV2.GetRangeResponse {
	resp := new(objectV2.GetRangeResponse)

	body := new(objectV2.GetRangeResponseBody)
	resp.SetBody(body)

	body.SetRangePart(info.ToV2())

	return resp
}

func setSplitInfoHeadResponse(info *objectSDK.SplitInfo, resp *objectV2.HeadResponse) {
	resp.GetBody().SetHeaderPart(info.ToV2())
}

func toHashResponse(typ refs.ChecksumType, res *getsvc.RangeHashRes) *objectV2.GetRangeHashResponse {
	resp := new(objectV2.GetRangeHashResponse)

	body := new(objectV2.GetRangeHashResponseBody)
	resp.SetBody(body)

	body.SetType(typ)
	body.SetHashList(res.Hashes())

	return resp
}

func toFullObjectHeader(hdr *object.Object) objectV2.GetHeaderPart {
	obj := hdr.ToV2()

	hs := new(objectV2.HeaderWithSignature)
	hs.SetHeader(obj.GetHeader())
	hs.SetSignature(obj.GetSignature())

	return hs
}

func toShortObjectHeader(hdr *object.Object) objectV2.GetHeaderPart {
	hdrV2 := hdr.ToV2().GetHeader()

	sh := new(objectV2.ShortHeader)
	sh.SetOwnerID(hdrV2.GetOwnerID())
	sh.SetCreationEpoch(hdrV2.GetCreationEpoch())
	sh.SetPayloadLength(hdrV2.GetPayloadLength())
	sh.SetVersion(hdrV2.GetVersion())
	sh.SetObjectType(hdrV2.GetObjectType())

	return sh
}
